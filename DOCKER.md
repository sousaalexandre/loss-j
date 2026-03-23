# Running with Docker

Step-by-step instructions to run the Loss-J application using Docker.

## Prerequisites

- Docker and Docker Compose installed ([Install Docker](https://docs.docker.com/get-docker/))
- OpenAI API key

## Steps

### 1. Configure Environment Variables

Create a `.env` file in the project root:

```bash
cp .env.example .env
```

Edit `.env` and add your OpenAI API key:

```ini
OPENAI_API_KEY=sk-...
```

### 2. Build the Docker Image

```bash
docker-compose build
```

### 3. Start the Container

```bash
docker-compose up
```

Or run in background:

```bash
docker-compose up -d
```

### 4. Access the Application

Open your browser at [localhost:8501](http://localhost:8501)

## Common Commands

**Stop the container:**
```bash
docker-compose down
```

**View logs:**
```bash
docker-compose logs -f
```

**Restart:**
```bash
docker-compose restart
```

**Rebuild after changes:**
```bash
docker-compose build --no-cache
docker-compose up -d
```
