# Beava examples

Runnable examples. Start here if you've just cloned the repo.

- [`fraud/`](fraud/) — the 60-second fraud-detection walkthrough. One
  pipeline definition, 200 pre-generated events, one command. Shows real
  sliding-window features (count, sum, avg, max, distinct-count, last)
  served in microseconds for a Zipfian-skewed mix of users. Start here:

  ```bash
  docker compose up -d
  bash examples/fraud/demo.sh
  ```

More examples will land here as the SDK grows. PRs welcome.
