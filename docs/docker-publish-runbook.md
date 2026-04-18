# Docker Hub Publishing Runbook (Beava v1.0-launch)

**Decision locks:**
- D-03 (Phase 47): tags `beavadb/beava:latest` and `beavadb/beava:0.1.0`
- D-04 (Phase 47): manual push at launch day; auto-push from CI is post-launch polish

---

## Prerequisites

- Docker Desktop (or Docker Engine) installed and running
- Write access to the `beavadb` organization on Docker Hub
- `git` on `main` with a clean working tree

---

## One-time setup

1. Create the `beavadb` organization on [hub.docker.com](https://hub.docker.com) if it does not yet exist.
2. Create the `beavadb/beava` repository (public, Apache-2.0).
3. Grant the release maintainer `write` (or `admin`) permission on `beavadb/beava`.
4. Log in from your workstation:

```bash
docker login -u <your-dockerhub-username>
# Enter your Docker Hub password or access token when prompted.
# Credentials are saved to ~/.docker/config.json.
```

---

## Publishing procedure

### 1. Verify the release tree is clean

```bash
git status                          # must show: "nothing to commit, working tree clean"
git log -1 --oneline                # confirm you are on the intended release commit
git branch --show-current           # must be `main`
```

### 2. Build + tag the image

```bash
# --platform linux/amd64 pins architecture for the first-release pass.
# Add linux/arm64 with `--platform linux/amd64,linux/arm64` in a follow-up
# (requires `docker buildx` with a multi-arch builder).

docker buildx build \
  --platform linux/amd64 \
  --tag beavadb/beava:latest \
  --tag beavadb/beava:0.1.0 \
  --load \
  .
```

> **Note:** `--load` materialises the image into the local Docker daemon so you
> can smoke-test it before pushing. Omit `--load` if you are using a remote
> buildx builder.

### 3. Smoke-test locally before pushing

```bash
docker run -d --rm -p 6900:6900 --name beava-release-smoke beavadb/beava:0.1.0
sleep 3
curl -fsS http://localhost:6900/health          # must return: {"status":"ok"}
docker stop beava-release-smoke
```

If the health check fails, **do not push**. Investigate with:

```bash
docker logs beava-release-smoke 2>&1 | tail -30
```

### 4. Push both tags to Docker Hub

```bash
docker push beavadb/beava:0.1.0
docker push beavadb/beava:latest
```

Both pushes are required: `:0.1.0` is the immutable release tag; `:latest`
is the floating tag users pull by default.

### 5. Verify the push from a clean cache

```bash
docker rmi beavadb/beava:latest beavadb/beava:0.1.0
docker pull beavadb/beava:latest

docker run -d --rm -p 6900:6900 --name beava-postpush-smoke beavadb/beava:latest
sleep 3
curl -fsS http://localhost:6900/health
docker stop beava-postpush-smoke
```

---

## Rollback

If a bad image was pushed as `:latest`, re-tag and push the known-good image:

```bash
# Identify the good digest from Docker Hub or local `docker images --digests`
docker pull beavadb/beava@sha256:<good-digest>
docker tag beavadb/beava@sha256:<good-digest> beavadb/beava:latest
docker push beavadb/beava:latest
```

Docker Hub retains all pushed layers by digest, so previously-good builds
remain recoverable via their `sha256:` reference even after `:latest` is moved.

---

## Post-launch automation (deferred per D-04)

A GitHub Actions workflow can automate multi-arch builds and pushes when a
`v*.*.*` tag is pushed. This is out of scope for v1.0-launch; track as a
post-launch polish item in LAUNCH.md.

Skeleton workflow (for reference — not wired yet):

```yaml
# .github/workflows/docker-publish.yml
on:
  push:
    tags: ["v*.*.*"]

jobs:
  push:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/login-action@v3
        with:
          username: ${{ secrets.DOCKERHUB_USERNAME }}
          password: ${{ secrets.DOCKERHUB_TOKEN }}
      - uses: docker/build-push-action@v6
        with:
          platforms: linux/amd64,linux/arm64
          push: true
          tags: |
            beavadb/beava:latest
            beavadb/beava:${{ github.ref_name }}
```
