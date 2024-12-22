# Redis-Backed Data Structures for Python

Redis-backed Python data structures that function like built-ins but store data on a Redis server, enabling shared access across distributed systems.

---

## 🚀 Features

- **Drop-in Replacements**: Use familiar Python data structures (`list`, `dict`, `set`, etc.) with minimal changes to your code.
- **Redis-Powered**: Data is stored on the Redis server, enabling easy sharing across processes, services, or systems.
- **Seamless Integration**: Works like native Python data structures while leveraging Redis for distributed use cases.

---

## 📥 Installation

Install the library using pip:

```bash
pip install py_redis_ds
```

---

## 💡 Usage

Here’s a quick example of how to use a Redis-backed list:

```python
from py_redis_ds.builtins import List
from redis import Redis

redis = Redis()

# Initialize a Redis-backed list
rlist = List(name="my_shared_list", redis=redis)

# Add elements
rlist.append("Hello")
rlist.append("World")

# Access elements
print(rlist[0])  # Output: Hello

# Iterate through the list
for item in rlist:
    print(item)

# Shared access in another script or service
rlist2 = List(name="my_shared_list", redis=redis)
print(rlist2[1])  # Output: World
```

---

## 📚 Supported Data Structures

| Data Structure | Redis-Backed Equivalent |
|----------------|-------------------------|
| `list`         | `py_redis_ds.builtins.List` |
| `dict`         | `py_redis_ds.builtins.Dict` |
| `set`          | `py_redis_ds.builtins.Set` |
| `queue.Queue`  | `py_redis_ds.queue.Queue` |
| `collections.deque` | `py_redis_ds.collections.Deque` |
| `collections.defaultdict` | `py_redis_ds.collections.DefaultDict` |
(Support for more data structures coming soon!)

---

## 🌟 Why Use Redis-Backed Structures?

- **Shared State**: Easily share data between processes, microservices, or distributed systems.
- **Scalability**: Offload data storage to Redis, reducing memory usage in individual processes.
- **Simplicity**: Use familiar Python syntax while leveraging Redis's power.

---

## 🤝 Contributing

Contributions are welcome! Please check out our [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## 📜 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

We’re excited to see how you use Redis-Backed Data Structures! If you have any questions or feedback, feel free to open an issue or start a discussion.

