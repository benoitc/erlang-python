# AI helper functions for Erlang examples
# This module is imported by the Erlang AI examples

import sys
import os

# Add this module's directory to path if needed
_this_dir = os.path.dirname(os.path.abspath(__file__))
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

_model = None
_llm_type = None
_llm_client = None


def _ensure_model():
    """Lazy load the embedding model."""
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer('all-MiniLM-L6-v2')
    return _model


def embed_texts(texts):
    """Embed a list of texts, returns list of embedding lists."""
    model = _ensure_model()
    # Handle bytes from Erlang
    texts = [t.decode('utf-8') if isinstance(t, bytes) else t for t in texts]
    embeddings = model.encode(texts, convert_to_numpy=True)
    return [emb.tolist() for emb in embeddings]


def embed_single(text):
    """Embed a single text."""
    model = _ensure_model()
    if isinstance(text, bytes):
        text = text.decode('utf-8')
    return model.encode(text, convert_to_numpy=True).tolist()


def model_info():
    """Get embedding model information."""
    model = _ensure_model()
    return {
        'name': 'all-MiniLM-L6-v2',
        'dimension': model.get_sentence_embedding_dimension(),
        'max_seq_length': model.max_seq_length
    }


def setup_llm():
    """Setup LLM (Ollama, OpenAI, or simulated)."""
    global _llm_type, _llm_client
    import os

    # Try Ollama first (local, free)
    try:
        import requests
        resp = requests.get('http://localhost:11434/api/tags', timeout=2)
        if resp.status_code == 200:
            models = resp.json().get('models', [])
            if models:
                _llm_type = 'ollama'
                return 'ollama'
    except:
        pass

    # Try OpenAI
    if os.environ.get('OPENAI_API_KEY'):
        try:
            from openai import OpenAI
            _llm_client = OpenAI()
            _llm_type = 'openai'
            return 'openai'
        except:
            pass

    # Fallback to simulated
    _llm_type = 'simulated'
    return 'simulated'


def get_llm_type():
    """Get current LLM type."""
    global _llm_type
    if _llm_type is None:
        setup_llm()
    return _llm_type or 'none'


def generate(question, context):
    """Generate an answer given question and context."""
    global _llm_type, _llm_client

    if _llm_type is None:
        setup_llm()

    if isinstance(question, bytes):
        question = question.decode('utf-8')
    if isinstance(context, bytes):
        context = context.decode('utf-8')

    prompt = 'Based on the following context, answer the question.\n'
    prompt += 'If the answer is not in the context, say "I don\'t have enough information."\n\n'
    prompt += 'Context:\n' + context + '\n\n'
    prompt += 'Question: ' + question + '\n\nAnswer:'

    if _llm_type == 'ollama':
        import requests
        resp = requests.post(
            'http://localhost:11434/api/generate',
            json={'model': 'llama3.2', 'prompt': prompt, 'stream': False},
            timeout=60
        )
        return resp.json()['response'].strip()

    elif _llm_type == 'openai':
        response = _llm_client.chat.completions.create(
            model='gpt-3.5-turbo',
            messages=[{'role': 'user', 'content': prompt}],
            max_tokens=200
        )
        return response.choices[0].message.content.strip()

    else:
        # Simulated response - return relevant part of context
        return 'Based on the provided context: ' + context[:300] + '...'


def chat(messages):
    """Send chat messages and get response."""
    global _llm_type, _llm_client

    if _llm_type is None:
        setup_llm()

    # Convert from Erlang format
    msgs = []
    for m in messages:
        if isinstance(m, dict):
            role = m.get('role', m.get(b'role', b'user'))
            content = m.get('content', m.get(b'content', b''))
            if isinstance(role, bytes):
                role = role.decode('utf-8')
            if isinstance(content, bytes):
                content = content.decode('utf-8')
            msgs.append({'role': role, 'content': content})

    if _llm_type == 'ollama':
        import requests
        prompt = '\n'.join([m['role'] + ': ' + m['content'] for m in msgs])
        prompt += '\nassistant:'
        resp = requests.post(
            'http://localhost:11434/api/generate',
            json={'model': 'llama3.2', 'prompt': prompt, 'stream': False},
            timeout=120
        )
        return resp.json()['response'].strip()

    elif _llm_type == 'openai':
        response = _llm_client.chat.completions.create(
            model='gpt-3.5-turbo',
            messages=msgs,
            max_tokens=500
        )
        return response.choices[0].message.content.strip()

    return 'No LLM available. Please install Ollama or set OPENAI_API_KEY.'
