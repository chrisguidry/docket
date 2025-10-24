#!/usr/bin/env python3
"""
Example: FastAPI Background Tasks with Docket

This example demonstrates how to integrate Docket with FastAPI to handle
background tasks that are offloaded from web request handlers. This pattern
is ideal for operations that are too slow to run synchronously during a
web request (sending emails, processing images, generating reports, etc.).

Why use Docket instead of FastAPI's built-in background_tasks?
--------------------------------------------------------------
FastAPI provides BackgroundTasks for simple fire-and-forget operations, but
Docket offers critical advantages for production systems:

- **Durability**: Tasks are persisted in Redis and survive server restarts,
  deployments, and crashes. FastAPI's background_tasks run in-memory and are
  lost if the server goes down.

- **Horizontal scaling**: Multiple worker processes across different machines
  can process tasks from the same queue. FastAPI's background_tasks only run
  in the web server process that created them.

- **Advanced features**: Docket provides scheduling (run tasks at specific times),
  retries with exponential backoff, task dependencies, and more. FastAPI's
  background_tasks are simple callables with no built-in retry or scheduling.

- **Observability**: Monitor queued, running, and completed tasks across your
  entire system. Track worker health and task performance.

Use Docket when you need reliability and scalability. Use FastAPI's background_tasks
for simple, non-critical operations where task loss on restart is acceptable.

Key patterns demonstrated:
- Using FastAPI's lifespan context manager to start/stop Docket worker
- Embedding a Docket worker within the web application process
- Dependency injection to access Docket from route handlers
- Scheduling background tasks from API endpoints

Architecture:
- The Docket worker runs in a background asyncio task alongside uvicorn
- Web requests return immediately after scheduling tasks
- Background tasks are processed concurrently by the embedded worker

Required dependencies:
    uv pip install pydocket fastapi uvicorn

To run:
    uv run -s examples/fastapi_background_tasks.py

To test:
    curl -X POST http://localhost:8000/create_user \\
         -H "Content-Type: application/json" \\
         -d '{"name": "Jane Doe", "email": "jane@example.com", "password": "secret"}'

You should see the endpoint return immediately (201 Created), then 1 second
later see the "Email sent" message in the server logs as the background task
executes.
"""

from contextlib import asynccontextmanager
import asyncio
from datetime import datetime
from typing import Annotated
from fastapi import Depends, FastAPI, Request
from pydantic import BaseModel

from docket import Docket, Worker

from common import run_redis

# Redis connection URL - will be overridden by main() during testing
redis_url = "redis://localhost:6379/0"


# ============================================================================
# Background Task Definition
# ============================================================================
# This is the function that will be executed as a background task. In a real
# application, this might send an actual email via SMTP, an email service API,
# or a message queue. Here we simulate a slow operation with asyncio.sleep().


async def send_email(email: str):
    """Simulates sending a welcome email to a new user."""
    print(f"Sending email to {email}", flush=True)
    await asyncio.sleep(1)  # Simulate slow I/O operation
    print(
        f"Email sent to {email} @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        flush=True,
    )


# ============================================================================
# FastAPI Lifespan Management
# ============================================================================
# FastAPI's lifespan context manager runs during application startup and
# shutdown. This is the perfect place to initialize Docket and start the
# background worker. The worker will run in a separate asyncio task alongside
# the web server, processing tasks as they're scheduled.


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages Docket and Worker lifecycle alongside FastAPI application."""
    worker_task: asyncio.Task[None] | None = None
    try:
        # Initialize Docket connection to Redis
        async with Docket(url=redis_url) as docket:
            # Store Docket instance in app state for access from route handlers
            app.state.docket = docket

            # Register our background task function with Docket
            docket.register(send_email)

            # Start the worker in a background asyncio task
            async with Worker(docket) as worker:
                # run_forever() processes tasks continuously
                worker_task = asyncio.create_task(worker.run_forever())

                # Yield control back to FastAPI - app is now running with
                # both the web server and background worker active
                yield
    finally:
        # Cleanup: gracefully shutdown the worker when app stops
        if worker_task:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass


# ============================================================================
# Dependency Injection Setup
# ============================================================================
# FastAPI's dependency injection system allows us to easily access the Docket
# instance from route handlers. This function extracts Docket from app state.


def get_docket(request: Request) -> Docket:
    """Dependency that provides access to the Docket instance."""
    return request.app.state.docket


# Initialize FastAPI app with our lifespan manager
# This ensures Docket worker starts when the app starts
app = FastAPI(lifespan=lifespan)


# ============================================================================
# API Route with Background Task
# ============================================================================
# This route demonstrates the typical pattern: handle the request quickly,
# schedule background work, and return immediately to the client.


class User(BaseModel):
    """User registration data."""

    name: str
    email: str
    password: str


@app.post("/create_user", status_code=201)
async def create_user(user: User, docket: Annotated[Docket, Depends(get_docket)]):
    """
    Create a new user and send welcome email in the background.

    The endpoint returns immediately after scheduling the email task.
    The actual email sending happens asynchronously in the background worker.
    """
    # Schedule the send_email task with the user's email address
    # This returns almost instantly - the task is queued but not yet executed
    await docket.add(send_email)(user.email)

    # Return 201 Created immediately - client doesn't wait for email to send
    return


# ============================================================================
# Test Harness
# ============================================================================
# For demonstration purposes, we embed a temporary Redis instance.
# In production, you would connect to your existing Redis server.


async def main():
    """Run the FastAPI app with an embedded test Redis instance."""
    # Start a temporary Redis instance for testing
    async with run_redis("7.4.2") as url:
        global redis_url
        redis_url = url

        import uvicorn

        # Use uvicorn's async API to run the server within our event loop
        config = uvicorn.Config(app, host="0.0.0.0", port=8000)
        server = uvicorn.Server(config)
        await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
