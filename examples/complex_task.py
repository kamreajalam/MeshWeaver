"""Example 2: a more complex, deterministic function — good for proving
cloudpickle round-trips nested/structured data correctly."""


def word_frequency(text: str) -> dict:
    """Return a sorted word-frequency table for `text`."""
    counts: dict[str, int] = {}
    for raw_word in text.lower().split():
        word = "".join(ch for ch in raw_word if ch.isalnum())
        if not word:
            continue
        counts[word] = counts.get(word, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


def is_prime(n: int) -> bool:
    if n < 2:
        return False
    for i in range(2, int(n ** 0.5) + 1):
        if n % i == 0:
            return False
    return True


def primes_up_to(n: int) -> list:
    return [i for i in range(2, n + 1) if is_prime(i)]
