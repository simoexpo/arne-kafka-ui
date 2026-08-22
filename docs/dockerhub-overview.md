# Arne — Kafka UI

Fast, honest Kafka UI for developers: topic monitoring, live message lookup,
consumer group insight — one container, no database, no agent.

![Browsing and live-tailing a topic in Arne](https://raw.githubusercontent.com/simoexpo/arne-kafka-ui/main/docs/images/hero-messages.png)

Arne aims to be a Kafka UI you can leave pointed at production: numbers you
can act on, an honest "don't know" where that is the truth, and as little
load on the cluster as we can manage.

- **Say what is known, and when it was measured.** Every metric carries the
  timestamp of its sample. Lag summed from a partial snapshot renders as
  `≥ 4.2k`, never as a confident total. A message that fails to decode
  appears in the results, loudly, instead of vanishing.
- **Cost the brokers as little as possible.** Every broker call is
  demand-driven, shared and bounded. Nothing polls in the background, and a
  second viewer of a page someone just loaded costs the brokers nothing.
  When nobody is looking, the only traffic left is the Kafka client
  library's own periodic housekeeping.

KIP-848 consumer groups are shown truthfully (the legacy describe API calls
them `Dead` while they consume happily — Arne doesn't).

## Quick start

```bash
cat > config.yaml <<'YAML'
clusters:
  - name: local
    bootstrap: localhost:9092
server: { port: 8080 }
YAML

docker run -p 8080:8080 -v $(pwd)/config.yaml:/etc/arne/config.yaml simoexpo/arne-kafka-ui:latest
```

Open http://localhost:8080. That is the whole installation. State lives in
Kafka or in memory, nowhere else.

SASL, TLS and Schema Registry are configured in the same file — see
[config.example.yaml](https://github.com/simoexpo/arne-kafka-ui/blob/main/config.example.yaml).
Passwords can be given as `${ENV_VAR}` and are redacted from every log line
and error message.

## Tags

| Tag | Meaning |
|---|---|
| `latest`, `X.Y`, `X.Y.Z` | Releases. A release is a promotion of a tested main build — the exact bits CI built and tested, retagged, never rebuilt. |
| `main-<sha>` | Every commit to main, published after the full test suite is green. The UI's corner shows the commit each build was made from. |

Images are published for `linux/amd64` and `linux/arm64`, both built natively.

## Links

- Source, docs and issues: [github.com/simoexpo/arne-kafka-ui](https://github.com/simoexpo/arne-kafka-ui)
- License: [Apache-2.0](https://github.com/simoexpo/arne-kafka-ui/blob/main/LICENSE)
