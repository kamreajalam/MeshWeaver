"""Example 3: a small ML-style calculation (linear regression via the
normal equation, hand-rolled) — no numpy/sklearn dependency, no external
API calls, so it's safe to ship as a remote task in a zero-dependency demo.
"""


def linear_regression_fit(xs: list, ys: list) -> dict:
    """Fit y = slope * x + intercept via least squares, pure Python."""
    n = len(xs)
    if n == 0 or n != len(ys):
        raise ValueError("xs and ys must be non-empty and equal length")

    mean_x = sum(xs) / n
    mean_y = sum(ys) / n

    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    denominator = sum((x - mean_x) ** 2 for x in xs)

    slope = numerator / denominator if denominator else 0.0
    intercept = mean_y - slope * mean_x

    predictions = [slope * x + intercept for x in xs]
    sse = sum((y - p) ** 2 for y, p in zip(ys, predictions))

    return {"slope": slope, "intercept": intercept, "sse": sse}


def k_means_1d(points: list, k: int = 2, iterations: int = 10) -> dict:
    """A tiny 1-D k-means implementation for demo purposes."""
    if not points or k <= 0:
        raise ValueError("points must be non-empty and k must be positive")

    centroids = sorted(points)[:: max(1, len(points) // k)][:k]
    assignments = [0] * len(points)

    for _ in range(iterations):
        for i, p in enumerate(points):
            assignments[i] = min(range(len(centroids)), key=lambda c: abs(p - centroids[c]))
        for c in range(len(centroids)):
            cluster = [points[i] for i in range(len(points)) if assignments[i] == c]
            if cluster:
                centroids[c] = sum(cluster) / len(cluster)

    return {"centroids": centroids, "assignments": assignments}
