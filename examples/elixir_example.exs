#!/usr/bin/env elixir
# Elixir Example - Using erlang_python from Elixir
#
# This example demonstrates:
#   - Calling Python functions from Elixir
#   - Using Erlang's concurrency with Python
#   - AI/ML integration patterns
#
# Prerequisites:
#   1. Build the project: rebar3 compile
#   2. Run from project root:
#      elixir --erl "-pa _build/default/lib/erlang_python/ebin" examples/elixir_example.exs
#
# For AI examples, also create a venv:
#   python3 -m venv /tmp/ai-venv
#   /tmp/ai-venv/bin/pip install sentence-transformers numpy

defmodule ElixirPythonExample do
  @moduledoc """
  Examples of using erlang_python from Elixir.

  The `py` module is an Erlang module that can be called directly
  from Elixir using the `:py` atom.
  """

  def run do
    IO.puts("\n=== Elixir + Python Integration Example ===\n")

    # Start the erlang_python application
    {:ok, _} = Application.ensure_all_started(:erlang_python)

    # Run demonstrations
    demo_basic_calls()
    demo_data_types()
    demo_erlang_callbacks()
    demo_parallel_processing()

    # Optional: AI demo (requires venv with sentence-transformers)
    case demo_ai_integration() do
      :ok -> :ok
      {:error, reason} -> IO.puts("  Skipped AI demo: #{inspect(reason)}")
    end

    IO.puts("\n=== Done ===\n")

    # Cleanup
    Application.stop(:erlang_python)
  end

  # ---------------------------------------------------------------------------
  # Demo 1: Basic Python Calls
  # ---------------------------------------------------------------------------

  def demo_basic_calls do
    IO.puts("--- Demo 1: Basic Python Calls ---\n")

    # Simple expression evaluation
    {:ok, result} = :py.eval("2 + 2")
    IO.puts("  2 + 2 = #{result}")

    # Call a Python module function
    {:ok, sqrt} = :py.call(:math, :sqrt, [16])
    IO.puts("  math.sqrt(16) = #{sqrt}")

    # With keyword arguments (as a map)
    {:ok, json} = :py.call(:json, :dumps, [%{name: "Elixir", version: 1.15}], %{indent: 2})
    IO.puts("  JSON output:\n#{json}")

    # Get Python version (import inline since workers are pooled)
    {:ok, version} = :py.eval("__import__('sys').version_info[:2]")
    IO.puts("  Python version: #{inspect(version)}")

    IO.puts("")
  end

  # ---------------------------------------------------------------------------
  # Demo 2: Data Type Conversion
  # ---------------------------------------------------------------------------

  def demo_data_types do
    IO.puts("--- Demo 2: Data Type Conversion ---\n")

    # Elixir -> Python -> Elixir
    test_values = [
      {:integer, 42},
      {:float, 3.14159},
      {:string, "Hello from Elixir!"},
      {:list, [1, 2, 3, 4, 5]},
      {:map, %{key: "value", nested: %{a: 1, b: 2}}},
      {:tuple_as_list, [1, "mixed", 3.14]}
    ]

    for {name, value} <- test_values do
      {:ok, result} = :py.eval("data", %{data: value})
      IO.puts("  #{name}: #{inspect(value)} -> #{inspect(result)}")
    end

    # Python list comprehension
    {:ok, squares} = :py.eval("[x**2 for x in range(1, 6)]")
    IO.puts("  List comprehension: #{inspect(squares)}")

    # Python dict
    {:ok, py_dict} = :py.eval("{'language': 'Elixir', 'vm': 'BEAM', 'awesome': True}")
    IO.puts("  Python dict: #{inspect(py_dict)}")

    IO.puts("")
  end

  # ---------------------------------------------------------------------------
  # Demo 3: Register Elixir/Erlang Functions for Python
  # ---------------------------------------------------------------------------

  def demo_erlang_callbacks do
    IO.puts("--- Demo 3: Erlang/Elixir Callbacks from Python ---\n")

    # Register an Elixir function that Python can call
    :py.register_function(:greet, fn [name] ->
      "Hello, #{name}! Greetings from Elixir!"
    end)

    :py.register_function(:factorial, fn [n] ->
      factorial(n)
    end)

    :py.register_function(:process_data, fn [data] ->
      # Elixir processing
      %{
        original: data,
        uppercased: String.upcase(to_string(data)),
        length: String.length(to_string(data)),
        processed_at: System.system_time(:millisecond)
      }
    end)

    # Call from Python
    {:ok, greeting} = :py.eval("__import__('erlang').call('greet', 'Python')")
    IO.puts("  Greeting: #{greeting}")

    {:ok, fact} = :py.eval("__import__('erlang').call('factorial', 10)")
    IO.puts("  factorial(10) = #{fact}")

    {:ok, processed} = :py.eval("__import__('erlang').call('process_data', 'elixir')")
    IO.puts("  Processed: #{inspect(processed)}")

    # Cleanup
    :py.unregister_function(:greet)
    :py.unregister_function(:factorial)
    :py.unregister_function(:process_data)

    IO.puts("")
  end

  defp factorial(0), do: 1
  defp factorial(n) when n > 0, do: n * factorial(n - 1)

  # ---------------------------------------------------------------------------
  # Demo 4: Parallel Processing with BEAM Processes
  # ---------------------------------------------------------------------------

  def demo_parallel_processing do
    IO.puts("--- Demo 4: Parallel Processing with BEAM ---\n")

    # Register a parallel map function
    :py.register_function(:parallel_map, fn [func_name, items] ->
      parent = self()

      # Spawn a process for each item
      refs_and_items =
        Enum.map(items, fn item ->
          ref = make_ref()
          spawn(fn ->
            result = execute_function(func_name, item)
            send(parent, {ref, result})
          end)
          {ref, item}
        end)

      # Collect results in order
      Enum.map(refs_and_items, fn {ref, _item} ->
        receive do
          {^ref, result} -> result
        after
          5000 -> {:error, :timeout}
        end
      end)
    end)

    # Register a slow computation
    :py.register_function(:slow_square, fn [n] ->
      Process.sleep(100)  # 100ms delay
      n * n
    end)

    items = Enum.to_list(1..10)

    # Sequential timing
    IO.puts("  Processing #{length(items)} items (100ms each)...")

    seq_start = System.monotonic_time(:millisecond)
    seq_results = Enum.map(items, fn n ->
      {:ok, r} = :py.eval("__import__('erlang').call('slow_square', n)", %{n: n})
      r
    end)
    seq_time = System.monotonic_time(:millisecond) - seq_start

    IO.puts("  Sequential: #{inspect(seq_results)}")
    IO.puts("  Sequential time: #{seq_time}ms")

    # Parallel timing
    par_start = System.monotonic_time(:millisecond)
    {:ok, par_results} = :py.eval(
      "__import__('erlang').call('parallel_map', 'slow_square', items)",
      %{items: items}
    )
    par_time = System.monotonic_time(:millisecond) - par_start

    IO.puts("  Parallel: #{inspect(par_results)}")
    IO.puts("  Parallel time: #{par_time}ms")

    speedup = seq_time / max(par_time, 1)
    IO.puts("  Speedup: #{Float.round(speedup, 1)}x faster!")

    # Cleanup
    :py.unregister_function(:parallel_map)
    :py.unregister_function(:slow_square)

    IO.puts("")
  end

  defp execute_function(:slow_square, n) do
    Process.sleep(100)
    n * n
  end

  defp execute_function(name, arg) when is_binary(name) do
    execute_function(String.to_atom(name), arg)
  end

  defp execute_function(_name, arg), do: arg

  # ---------------------------------------------------------------------------
  # Demo 5: AI Integration (Optional)
  # ---------------------------------------------------------------------------

  def demo_ai_integration do
    IO.puts("--- Demo 5: AI Integration (Optional) ---\n")

    venv_path = "/tmp/ai-venv"

    case :py.activate_venv(venv_path) do
      :ok ->
        IO.puts("  Activated venv: #{venv_path}")

        # Check if sentence-transformers is available
        case :py.eval("__import__('sentence_transformers')") do
          {:ok, _} ->
            run_ai_demo()
            :py.deactivate_venv()
            :ok

          {:error, _} ->
            :py.deactivate_venv()
            {:error, "sentence-transformers not installed"}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp run_ai_demo do
    # Add examples dir to Python path and use ai_helpers module
    examples_dir = Path.join(File.cwd!(), "examples")
    {:ok, _} = :py.eval(
      "(__import__('sys').path.insert(0, path) if path not in __import__('sys').path else None, True)[1]",
      %{path: examples_dir}
    )

    IO.puts("  Loading embedding model...")

    # Generate embeddings
    texts = [
      "Elixir is a dynamic, functional language for building scalable applications.",
      "Python is widely used for machine learning and data science.",
      "The BEAM VM provides fault tolerance and concurrency.",
      "Neural networks are computational models inspired by the brain."
    ]

    IO.puts("  Generating embeddings for #{length(texts)} texts...")
    {:ok, embeddings} = :py.call(:ai_helpers, :embed_texts, [texts])

    IO.puts("  Generated #{length(embeddings)} embeddings")
    IO.puts("  Embedding dimension: #{length(hd(embeddings))}")

    # Semantic search
    query = "concurrent programming"
    IO.puts("\n  Searching for: \"#{query}\"")
    {:ok, query_emb} = :py.call(:ai_helpers, :embed_single, [query])

    # Calculate similarities
    similarities =
      texts
      |> Enum.zip(embeddings)
      |> Enum.map(fn {text, emb} ->
        {text, cosine_similarity(query_emb, emb)}
      end)
      |> Enum.sort_by(fn {_, score} -> score end, :desc)

    IO.puts("  Results:")
    for {text, score} <- similarities do
      IO.puts("    [#{Float.round(score, 3)}] #{String.slice(text, 0, 50)}...")
    end

    IO.puts("")
  end

  defp cosine_similarity(a, b) do
    dot = Enum.zip(a, b) |> Enum.map(fn {x, y} -> x * y end) |> Enum.sum()
    norm_a = a |> Enum.map(&(&1 * &1)) |> Enum.sum() |> :math.sqrt()
    norm_b = b |> Enum.map(&(&1 * &1)) |> Enum.sum() |> :math.sqrt()
    dot / (norm_a * norm_b)
  end
end

# Run the example
ElixirPythonExample.run()
