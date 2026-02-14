# Add AI to Your Erlang App

This guide shows how to integrate AI and machine learning capabilities into your Erlang application using erlang_python.

## Overview

Erlang excels at building distributed, fault-tolerant systems. Python dominates the AI/ML ecosystem with libraries like PyTorch, TensorFlow, sentence-transformers, and OpenAI clients. erlang_python bridges these worlds, letting you:

- Generate text embeddings for semantic search
- Call LLM APIs (OpenAI, Anthropic, local models)
- Run inference with pre-trained models
- Build RAG (Retrieval-Augmented Generation) systems

## Setup

### 1. Create a Virtual Environment

```bash
# Create venv with AI dependencies
python3 -m venv ai_venv
source ai_venv/bin/activate

# Install common AI libraries
pip install sentence-transformers numpy openai anthropic
```

### 2. Activate in Erlang

```erlang
1> application:ensure_all_started(erlang_python).
{ok, [erlang_python]}

2> py:activate_venv(<<"/path/to/ai_venv">>).
ok
```

## Text Embeddings

Embeddings convert text into numerical vectors, enabling semantic search, clustering, and similarity comparisons.

### Using sentence-transformers

```erlang
%% Load a model (do this once at startup)
init_embedding_model() ->
    py:exec(<<"
from sentence_transformers import SentenceTransformer
model = SentenceTransformer('all-MiniLM-L6-v2')
">>).

%% Generate embedding for a single text
embed(Text) ->
    {ok, Embedding} = py:eval(
        <<"model.encode(text).tolist()">>,
        #{text => Text}
    ),
    Embedding.

%% Generate embeddings for multiple texts (more efficient)
embed_batch(Texts) ->
    {ok, Embeddings} = py:eval(
        <<"model.encode(texts).tolist()">>,
        #{texts => Texts}
    ),
    Embeddings.
```

### Example: Semantic Search

```erlang
-module(semantic_search).
-export([index/1, search/2]).

%% Index documents with their embeddings
index(Documents) ->
    Embeddings = embed_batch(Documents),
    lists:zip(Documents, Embeddings).

%% Search for similar documents
search(Query, Index) ->
    QueryEmb = embed(Query),
    Scored = [{Doc, cosine_similarity(QueryEmb, DocEmb)}
              || {Doc, DocEmb} <- Index],
    lists:reverse(lists:keysort(2, Scored)).

%% Cosine similarity in Erlang
cosine_similarity(A, B) ->
    Dot = lists:sum([X * Y || {X, Y} <- lists:zip(A, B)]),
    NormA = math:sqrt(lists:sum([X * X || X <- A])),
    NormB = math:sqrt(lists:sum([X * X || X <- B])),
    Dot / (NormA * NormB).
```

### Example: Using the Search

```erlang
1> semantic_search:init_embedding_model().
ok

2> Docs = [
    <<"Erlang is great for building distributed systems">>,
    <<"Python excels at machine learning">>,
    <<"The BEAM VM provides fault tolerance">>,
    <<"Neural networks require GPU acceleration">>
].

3> Index = semantic_search:index(Docs).

4> semantic_search:search(<<"concurrent programming">>, Index).
[{<<"Erlang is great for building distributed systems">>, 0.42},
 {<<"The BEAM VM provides fault tolerance">>, 0.38},
 ...]
```

## Calling LLM APIs

### OpenAI

```erlang
%% Initialize OpenAI client
init_openai() ->
    py:exec(<<"
import os
from openai import OpenAI
client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
">>).

%% Chat completion
chat(Messages) ->
    %% Convert Erlang messages to Python format
    PyMessages = [#{role => Role, content => Content}
                  || {Role, Content} <- Messages],
    {ok, Response} = py:eval(<<"
response = client.chat.completions.create(
    model='gpt-4',
    messages=messages
)
response.choices[0].message.content
">>, #{messages => PyMessages}),
    Response.

%% Usage
chat([{system, <<"You are a helpful assistant.">>},
      {user, <<"What is Erlang?">>}]).
%% => <<"Erlang is a programming language designed for...">>
```

### Anthropic Claude

```erlang
init_anthropic() ->
    py:exec(<<"
import os
import anthropic
client = anthropic.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY'))
">>).

claude_chat(Prompt) ->
    {ok, Response} = py:eval(<<"
message = client.messages.create(
    model='claude-sonnet-4-20250514',
    max_tokens=1024,
    messages=[{'role': 'user', 'content': prompt}]
)
message.content[0].text
">>, #{prompt => Prompt}),
    Response.
```

### Local Models with Ollama

```erlang
init_ollama() ->
    py:exec(<<"
import requests

def ollama_generate(prompt, model='llama2'):
    response = requests.post(
        'http://localhost:11434/api/generate',
        json={'model': model, 'prompt': prompt, 'stream': False}
    )
    return response.json()['response']
">>).

ollama_chat(Prompt) ->
    {ok, Response} = py:eval(
        <<"ollama_generate(prompt)">>,
        #{prompt => Prompt}
    ),
    Response.
```

## Building a RAG System

Retrieval-Augmented Generation combines semantic search with LLM generation.

```erlang
-module(rag).
-export([init/0, add_document/2, query/2]).

-record(state, {
    index = [] :: [{binary(), [float()]}]
}).

init() ->
    %% Initialize embedding model
    py:exec(<<"
from sentence_transformers import SentenceTransformer
from openai import OpenAI
import os

embedder = SentenceTransformer('all-MiniLM-L6-v2')
llm = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))

def embed(text):
    return embedder.encode(text).tolist()

def embed_batch(texts):
    return embedder.encode(texts).tolist()

def generate(prompt, context):
    response = llm.chat.completions.create(
        model='gpt-4',
        messages=[
            {'role': 'system', 'content': f'Use this context to answer: {context}'},
            {'role': 'user', 'content': prompt}
        ]
    )
    return response.choices[0].message.content
">>),
    #state{}.

add_document(Doc, #state{index = Index} = State) ->
    {ok, Embedding} = py:eval(<<"embed(doc)">>, #{doc => Doc}),
    State#state{index = [{Doc, Embedding} | Index]}.

query(Question, #state{index = Index}) ->
    %% 1. Embed the question
    {ok, QueryEmb} = py:eval(<<"embed(q)">>, #{q => Question}),

    %% 2. Find top-k similar documents
    Scored = [{Doc, cosine_sim(QueryEmb, DocEmb)} || {Doc, DocEmb} <- Index],
    TopK = lists:sublist(lists:reverse(lists:keysort(2, Scored)), 3),
    Context = iolist_to_binary([Doc || {Doc, _} <- TopK]),

    %% 3. Generate answer with context
    {ok, Answer} = py:eval(
        <<"generate(question, context)">>,
        #{question => Question, context => Context}
    ),
    Answer.

cosine_sim(A, B) ->
    Dot = lists:sum([X * Y || {X, Y} <- lists:zip(A, B)]),
    NormA = math:sqrt(lists:sum([X * X || X <- A])),
    NormB = math:sqrt(lists:sum([X * X || X <- B])),
    Dot / (NormA * NormB).
```

### Using the RAG System

```erlang
1> State0 = rag:init().

2> State1 = rag:add_document(<<"Erlang was created at Ericsson in 1986.">>, State0).
3> State2 = rag:add_document(<<"The BEAM VM runs Erlang and Elixir code.">>, State1).
4> State3 = rag:add_document(<<"OTP provides behaviors like gen_server.">>, State2).

5> rag:query(<<"When was Erlang created?">>, State3).
<<"Erlang was created at Ericsson in 1986.">>
```

## Parallel Embedding with Sub-interpreters

For high-throughput embedding, use parallel execution:

```erlang
%% Embed many documents in parallel
embed_parallel(Documents) ->
    %% Split into batches
    BatchSize = 100,
    Batches = partition(Documents, BatchSize),

    %% Build parallel calls
    Calls = [{mymodule, embed_batch, [Batch]} || Batch <- Batches],

    %% Execute in parallel across sub-interpreters
    {ok, Results} = py:parallel(Calls),

    %% Flatten results
    lists:flatten([R || {ok, R} <- Results]).

partition([], _) -> [];
partition(L, N) ->
    {H, T} = lists:split(min(N, length(L)), L),
    [H | partition(T, N)].
```

## Async LLM Calls

For non-blocking LLM calls:

```erlang
%% Start async LLM call
ask_async(Question) ->
    py:call_async('__main__', generate, [Question, <<"">>]).

%% Gather multiple responses
ask_many(Questions) ->
    Refs = [ask_async(Q) || Q <- Questions],
    [py:await(Ref, 30000) || Ref <- Refs].
```

## Streaming LLM Responses

For streaming responses from LLMs:

```erlang
init_streaming() ->
    py:exec(<<"
from openai import OpenAI
import os

client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))

def stream_chat(prompt):
    stream = client.chat.completions.create(
        model='gpt-4',
        messages=[{'role': 'user', 'content': prompt}],
        stream=True
    )
    for chunk in stream:
        if chunk.choices[0].delta.content:
            yield chunk.choices[0].delta.content
">>).

stream_response(Prompt) ->
    {ok, Chunks} = py:stream('__main__', stream_chat, [Prompt]),
    %% Chunks is a list of text fragments
    iolist_to_binary(Chunks).
```

## Performance Tips

### 1. Batch Operations

```erlang
%% Slow: one call per embedding
[embed(Doc) || Doc <- Documents].

%% Fast: batch embedding
embed_batch(Documents).
```

### 2. Reuse Models

```erlang
%% Load model once at startup
init() ->
    py:exec(<<"model = SentenceTransformer('...')">>).

%% Reuse in each request
embed(Text) ->
    py:eval(<<"model.encode(text).tolist()">>, #{text => Text}).
```

### 3. Use GPU When Available

```erlang
init_gpu_model() ->
    py:exec(<<"
import torch
from sentence_transformers import SentenceTransformer

device = 'cuda' if torch.cuda.is_available() else 'cpu'
model = SentenceTransformer('all-MiniLM-L6-v2', device=device)
">>).
```

### 4. Monitor Rate Limits

```erlang
%% Check current load before heavy operations
check_capacity() ->
    Current = py_semaphore:current(),
    Max = py_semaphore:max_concurrent(),
    case Current / Max of
        Ratio when Ratio > 0.8 ->
            {error, high_load};
        _ ->
            ok
    end.
```

## Error Handling

```erlang
safe_embed(Text) ->
    try
        case py:eval(<<"model.encode(text).tolist()">>, #{text => Text}) of
            {ok, Embedding} -> {ok, Embedding};
            {error, Reason} -> {error, {python_error, Reason}}
        end
    catch
        error:timeout -> {error, timeout}
    end.

%% With retry
embed_with_retry(Text, Retries) when Retries > 0 ->
    case safe_embed(Text) of
        {ok, _} = Result -> Result;
        {error, _} ->
            timer:sleep(1000),
            embed_with_retry(Text, Retries - 1)
    end;
embed_with_retry(_, 0) ->
    {error, max_retries}.
```

## Complete Example: AI-Powered Search Service

```erlang
-module(ai_search).
-behaviour(gen_server).

-export([start_link/0, index/1, search/2]).
-export([init/1, handle_call/3, handle_cast/2]).

-record(state, {
    documents = #{} :: #{binary() => [float()]}
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

index(Documents) ->
    gen_server:call(?MODULE, {index, Documents}, 60000).

search(Query, TopK) ->
    gen_server:call(?MODULE, {search, Query, TopK}, 10000).

init([]) ->
    %% Initialize embedding model
    ok = py:exec(<<"
from sentence_transformers import SentenceTransformer
model = SentenceTransformer('all-MiniLM-L6-v2')
">>),
    {ok, #state{}}.

handle_call({index, Documents}, _From, State) ->
    {ok, Embeddings} = py:eval(
        <<"model.encode(docs).tolist()">>,
        #{docs => Documents}
    ),
    NewDocs = maps:from_list(lists:zip(Documents, Embeddings)),
    {reply, ok, State#state{documents = maps:merge(State#state.documents, NewDocs)}};

handle_call({search, Query, TopK}, _From, #state{documents = Docs} = State) ->
    {ok, QueryEmb} = py:eval(
        <<"model.encode(q).tolist()">>,
        #{q => Query}
    ),
    Scored = [{Doc, cosine_sim(QueryEmb, Emb)} || {Doc, Emb} <- maps:to_list(Docs)],
    Results = lists:sublist(lists:reverse(lists:keysort(2, Scored)), TopK),
    {reply, {ok, Results}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

cosine_sim(A, B) ->
    Dot = lists:sum([X * Y || {X, Y} <- lists:zip(A, B)]),
    NormA = math:sqrt(lists:sum([X * X || X <- A])),
    NormB = math:sqrt(lists:sum([X * X || X <- B])),
    Dot / (NormA * NormB).
```

## See Also

- [Getting Started](getting-started.md) - Basic usage
- [Type Conversion](type-conversion.md) - How data is converted
- [Scalability](scalability.md) - Parallel execution and rate limiting
- [Streaming](streaming.md) - Working with generators
