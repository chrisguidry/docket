# API Reference

This page contains the automatically generated API documentation for docket.

## Core Components

### Docket

The main interface for creating and managing tasks.

::: docket.Docket

### Worker

Worker implementation for processing tasks.

::: docket.Worker

### Execution

Task execution and state management.

::: docket.Execution

## Task Dependencies

Core dependency injection and task configuration utilities.

::: docket.dependencies.Depends
::: docket.dependencies.TaskArgument
::: docket.dependencies.TaskKey
::: docket.dependencies.TaskLogger

## Retry and Timeout Policies

Task retry and timeout configuration.

::: docket.dependencies.Retry
::: docket.dependencies.ExponentialRetry
::: docket.dependencies.Timeout

## Task Lifecycle

Task lifecycle and perpetual task utilities.

::: docket.dependencies.Perpetual
::: docket.dependencies.CurrentDocket
::: docket.dependencies.CurrentExecution
::: docket.dependencies.CurrentWorker

## Instrumentation

Task logging and instrumentation utilities.

::: docket.annotations.Logged
