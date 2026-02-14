# Helper module for sentence-transformers embeddings
# Used by embedder_example.erl

from sentence_transformers import SentenceTransformer

_model = None

def get_model():
    """Lazy load the model."""
    global _model
    if _model is None:
        _model = SentenceTransformer('all-MiniLM-L6-v2')
    return _model

def embed(texts):
    """Embed one or more texts, returns list of embeddings."""
    model = get_model()

    # Handle single string/bytes or list
    if isinstance(texts, (str, bytes)):
        if isinstance(texts, bytes):
            texts = texts.decode('utf-8')
        texts = [texts]
        single = True
    else:
        # Convert to list of strings (handling bytes from Erlang)
        texts = [t.decode('utf-8') if isinstance(t, bytes) else t for t in texts]
        single = False

    embeddings = model.encode(texts, convert_to_numpy=True)

    # Always convert to nested Python list for consistent Erlang conversion
    result = [emb.tolist() for emb in embeddings]

    if single:
        return result[0]  # Return single embedding for single input
    return result

def model_info():
    """Get model information."""
    model = get_model()
    return {
        'name': 'all-MiniLM-L6-v2',
        'dimension': model.get_sentence_embedding_dimension(),
        'max_seq_length': model.max_seq_length
    }
