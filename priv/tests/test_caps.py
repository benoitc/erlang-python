"""Unit tests for the capability resolver and the address matcher.

These run without a VM, so the containment rules can be read and changed
without a Common Test round trip. `py_isolated_caps_SUITE` covers the same
ground through a real child.

    cd priv && python3 -m unittest tests.test_caps
"""

import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from _erlang_impl import _caps  # noqa: E402


class PathContainment(unittest.TestCase):
    """The tree is the one `wasi_SUITE` uses, so the cases line up."""

    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp()
        cls.data = os.path.join(cls.root, 'data')
        cls.secret = os.path.join(cls.root, 'secret')
        os.makedirs(os.path.join(cls.data, 'sub'))
        os.makedirs(cls.secret)
        _write(os.path.join(cls.data, 'note.txt'), 'inside')
        _write(os.path.join(cls.data, 'sub', 'deep.txt'), 'deep')
        _write(os.path.join(cls.secret, 'key.txt'), 'secret')
        os.symlink(os.path.join(cls.secret, 'key.txt'),
                   os.path.join(cls.data, 'escape'))
        os.symlink(cls.secret, os.path.join(cls.data, 'outdir'))
        os.symlink('note.txt', os.path.join(cls.data, 'here'))
        os.symlink('loop', os.path.join(cls.data, 'loop'))
        cls.grant = _caps._Grant(cls.data, 'write')
        # The enforcement lives in the factory's closure, so that is what a
        # test drives; there is nothing at module level to call.
        state = _caps._State()
        state.dirs = [cls.grant]
        cls.enf = _caps._make_enforcer(state)

    @classmethod
    def tearDownClass(cls):
        os.close(cls.grant.fd)
        shutil.rmtree(cls.root, ignore_errors=True)

    def reaches(self, rel, follow=True):
        """Does `rel` resolve inside the grant?"""
        try:
            fd, _comp, owned = self.enf.walk(self.grant, rel, follow_last=follow)
        except _caps.CapabilityError:
            return False
        except OSError:
            return True          # inside the grant, simply not there
        if owned:
            os.close(fd)
        return True

    def test_a_plain_name_resolves(self):
        self.assertTrue(self.reaches('note.txt'))
        self.assertTrue(self.reaches('./note.txt'))
        self.assertTrue(self.reaches('sub/deep.txt'))

    def test_parent_traversal_is_refused(self):
        self.assertFalse(self.reaches('../secret/key.txt'))
        self.assertFalse(self.reaches('..'))

    def test_partial_traversal_is_refused(self):
        self.assertFalse(self.reaches('sub/../../secret/key.txt'))
        # And the half of it that is legal really is legal, or the assertion
        # above would hold just as well with `..` refused outright.
        self.assertTrue(self.reaches('sub/../note.txt'))

    def test_a_symlink_out_is_refused(self):
        self.assertFalse(self.reaches('escape'))

    def test_a_symlinked_directory_prefix_is_refused(self):
        self.assertFalse(self.reaches('outdir/key.txt'))

    def test_a_symlink_inside_is_followed(self):
        self.assertTrue(self.reaches('here'))

    def test_a_cycle_is_refused_rather_than_followed(self):
        self.assertFalse(self.reaches('loop'))

    def test_a_name_is_not_followed_when_the_caller_names_it(self):
        # Removing a link that leads out of the grant removes something
        # inside the grant, so naming it is allowed where following is not.
        self.assertFalse(self.reaches('escape', follow=True))
        self.assertTrue(self.reaches('escape', follow=False))

    def test_a_missing_name_is_not_a_containment_answer(self):
        self.assertTrue(self.reaches('missing.txt'))
        self.assertFalse(self.reaches('../missing.txt'))

    def test_the_walk_leaks_no_descriptors(self):
        before = _lowest_free_fd()
        for _ in range(200):
            for rel in ('note.txt', 'escape', 'loop', 'sub/../note.txt',
                        '../secret/key.txt', 'outdir/key.txt'):
                self.reaches(rel)
        self.assertLessEqual(_lowest_free_fd(), before + 1)


class ModuleState(unittest.TestCase):
    """The grants must not be reachable through this module.

    An enforcement decision that reads a module attribute is one any Python
    in the process can switch off by assigning to it.
    """

    def test_no_lever_is_exported(self):
        self.assertFalse(hasattr(_caps, 'allow_path'))

    def test_nothing_on_the_decision_path_lives_at_module_level(self):
        # A name the hook resolves when it runs is a name any Python in the
        # process can rebind, so none of these may exist here.
        for name in ('_walk', '_contained', '_check_path', '_writes',
                     '_net_allows', '_local', '_state'):
            self.assertFalse(hasattr(_caps, name), name)

    def test_the_decision_path_loads_no_module_global(self):
        # Not `co_names`, which also lists attribute names: what matters is
        # what the code actually loads from the module's namespace, because
        # that is what an assignment to this module would change.
        import dis
        enf = _caps._make_enforcer(_caps._State())
        for part in ('hook', 'walk', 'contained', 'check_path', 'writes'):
            code = getattr(enf, part).__code__
            loaded = {i.argval for i in dis.get_instructions(code)
                      if i.opname == 'LOAD_GLOBAL'}
            self.assertEqual(loaded & set(vars(_caps)), set(), part)

    def test_grants_returns_a_copy(self):
        # What `grants()` hands back is something to look at, so mutating it
        # must not reach anything.
        before = _caps.grants()
        if before is None:
            self.skipTest('no capability set installed in this process')
        before['dirs'].append(('/etc', 'write'))
        self.assertNotIn(('/etc', 'write'), _caps.grants()['dirs'])


class WriteIntent(unittest.TestCase):
    """Which opens need a write grant."""

    def setUp(self):
        self.writes = _caps._make_enforcer(_caps._State()).writes

    def test_modes(self):
        for mode in ('w', 'a', 'x', 'r+', 'w+b', 'rb+'):
            self.assertTrue(self.writes(mode, 0), mode)
        for mode in ('r', 'rb', 'rt'):
            self.assertFalse(self.writes(mode, 0), mode)

    def test_flags(self):
        for flag in (os.O_WRONLY, os.O_RDWR, os.O_CREAT, os.O_TRUNC,
                     os.O_APPEND, os.O_RDONLY | os.O_CREAT):
            self.assertTrue(self.writes(None, flag), flag)
        self.assertFalse(self.writes(None, os.O_RDONLY))

    def test_an_unreadable_intent_is_taken_as_a_write(self):
        self.assertTrue(self.writes(None, None))


class AddressMatching(unittest.TestCase):
    """The rules erlang_wasm's `wasi_net_SUITE` checks, matched here."""

    @staticmethod
    def grant(connect=(), listen=(), resolve=False):
        return _caps._parse_net({'connect': list(connect),
                                 'listen': list(listen),
                                 'resolve': resolve})

    @staticmethod
    def rule(cidr, lo, hi, proto='tcp'):
        return {'proto': proto, 'cidr': cidr, 'ports': [lo, hi]}

    def allows(self, net, addr, port, kind='connect', dgram=False):
        # The matcher lives in the enforcer, so a test builds one over the
        # grant it wants rather than poking module state.
        state = _caps._State()
        state.net = net
        enf = _caps._make_enforcer(state)
        event = 'socket.bind' if kind == 'listen' else 'socket.connect'
        try:
            enf.hook(event, (_FakeSocket(dgram), (addr, port)))
            return True
        except _caps.CapabilityError:
            return False

    def test_a_network_and_a_port_range(self):
        g = self.grant(connect=[self.rule('10.0.0.0/8', 8000, 8099)])
        self.assertTrue(self.allows(g, '10.1.2.3', 8000))
        self.assertTrue(self.allows(g, '10.255.255.255', 8099))
        self.assertFalse(self.allows(g, '11.0.0.1', 8000))
        self.assertFalse(self.allows(g, '10.1.2.3', 8100))
        self.assertFalse(self.allows(g, '10.1.2.3', 7999))

    def test_ipv4_mapped_ipv6_is_the_same_address(self):
        # A matcher comparing text would let this past a v4 rule.
        g = self.grant(connect=[self.rule('127.0.0.0/8', 80, 80)])
        self.assertTrue(self.allows(g, '::ffff:127.0.0.1', 80))
        self.assertFalse(self.allows(g, '::1', 80))

    def test_udp_and_tcp_are_separate(self):
        g = self.grant(connect=[self.rule('127.0.0.1/32', 53, 53, 'udp')])
        self.assertTrue(self.allows(g, '127.0.0.1', 53, dgram=True))
        self.assertFalse(self.allows(g, '127.0.0.1', 53))

    def test_connect_and_listen_are_separate(self):
        g = self.grant(connect=[self.rule('127.0.0.1/32', 80, 80)])
        self.assertTrue(self.allows(g, '127.0.0.1', 80, kind='connect'))
        self.assertFalse(self.allows(g, '127.0.0.1', 80, kind='listen'))

    def test_a_wildcard_really_is_a_wildcard(self):
        # Nothing is denied implicitly: 0.0.0.0/0 includes the link-local
        # and cloud metadata addresses, and this does not second-guess it.
        g = self.grant(connect=[self.rule('0.0.0.0/0', 0, 65535)])
        self.assertTrue(self.allows(g, '169.254.169.254', 80))

    def test_a_name_matches_nothing(self):
        # A rule names addresses, so an unresolved name cannot match one.
        g = self.grant(connect=[self.rule('0.0.0.0/0', 0, 65535)])
        self.assertFalse(self.allows(g, 'example.com', 80))

    def test_no_grant_allows_nothing(self):
        self.assertFalse(self.allows(None, '127.0.0.1', 80))


class _FakeSocket:
    def __init__(self, dgram):
        import socket
        self.type = socket.SOCK_DGRAM if dgram else socket.SOCK_STREAM


def _write(path, text):
    with open(path, 'w') as fh:
        fh.write(text)


def _lowest_free_fd():
    fd = os.dup(0)
    os.close(fd)
    return fd


if __name__ == '__main__':
    unittest.main()
