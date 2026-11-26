# 0010: The two latest OTP releases are supported

Since 5.1.0. Code: `rebar.config` (`minimum_otp_vsn`),
`.github/workflows/ci.yml`.

## Situation

The floor went up to OTP 28 in 3.1.0 and came back down to 27 in 3.1.1,
because nothing in the code needed 28. Nothing stops it from moving again
the next time someone asks. OTP itself supports its last three releases:
it still patches 27 until OTP 30 ships, so 27 is narrower than OTP's own
window, but CI cannot keep testing three releases for nothing.

## Decision

erlang_python supports the two latest OTP releases, with no upper bound.
When a new release ships, CI adds its rows and the next minor release
raises the floor to the release before it. `minimum_otp_vsn` in
`rebar.config` is the check: rebar3 refuses to build on an older release,
so nobody runs a combination CI no longer tests. There is no source guard;
without the CI row the older release would be untested, not broken.

## Consequences

- A user on the oldest release OTP still patches stays on the previous
  minor release (5.0.x for OTP 27).
- Code may use features of the floor release without a fallback, once CI
  covers it on every OS in the matrix.
