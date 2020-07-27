defmodule Rox.Native do
  use Rustler, otp_app: :rox, crate: "rox_nif"

  @dialyzer {:nowarn_function, [__init__: 0]}

  def open(_, _, _, _) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> {:ok, ""}
      2 -> {:error, "Invalid argument: Column family not found: Something"}
    end
  end

  def count(_) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> 0
    end
  end

  def flush(_) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> {:ok, ""}
      2 -> {:error, ""}
    end
  end

  def compact(_) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> {:ok, ""}
      2 -> {:error, ""}
    end
  end

  def create_cf(_, _, _) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> {:ok, ""}
      2 -> {:error, ""}
    end
  end

  def list_cf(_, _) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> {:ok, ""}
      2 -> {:error, ""}
    end
  end

  def put(_, _, _, _) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> :ok
    end
  end

  def put_cf(_, _, _, _, _) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> :ok
    end
  end

  def get(_, _) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> {:ok, ""}
    end
  end

  def get_cf(_, _, _) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> {:ok, ""}
    end
  end

  def delete(_, _, _) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> :ok
    end
  end

  def delete_cf(_, _, _, _) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> :ok
    end
  end

  def batch_write(_, _) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> :ok
      2 -> {:error, ""}
    end
  end

  def property(_, _) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> :ok
      2 -> {:error, ""}
    end
  end

  def property_cf(_, _, _) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> :ok
      2 -> {:error, ""}
    end
  end

  def property_int(_, _) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> :ok
      2 -> {:error, ""}
    end
  end

  def property_int_cf(_, _, _) do
    case :erlang.phash2(1, 1) do
      0 -> raise "Nif not loaded"
      1 -> :ok
      2 -> {:error, ""}
    end
  end
end
