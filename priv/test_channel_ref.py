# Test module for channel reference passing via py:call

def identity(x):
    """Return the argument unchanged."""
    return x

def get_channel_type(ch_ref):
    """Return the type name of the channel reference."""
    return type(ch_ref).__name__

def store_and_return(ch_ref):
    """Store in a list and return."""
    container = [ch_ref]
    return container[0]
