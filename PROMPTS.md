# ChatGPT Prompts – Flight Streaming System

This file documents the AI prompts used or intended to support the development of the **Flight Streaming System** project.

> **Important:** The prompts below are project-specific prompt templates. Keep only the prompts you actually used, and edit their wording to match your original ChatGPT conversations. Do not present a template as an exact historical prompt unless it matches what you sent.

## 1. Project Planning and Architecture

```text
I am developing a real-time flight monitoring and streaming system. Help me design
its end-to-end architecture. Explain the responsibilities of each component, how
flight data moves through the system, and how the components communicate. Consider
Python, the OpenSky Network API, Apache Kafka, SQLite, and Streamlit where appropriate.
Also identify likely failure points and suggest a practical development sequence.
```

## 2. OpenSky Network API Integration

```text
Help me write Python code to retrieve aircraft state-vector data from the OpenSky
Network API. Explain the API response fields and show how to handle missing values,
request failures, rate limits, timeouts, and authentication requirements. Keep the
API integration separate from the rest of the application so it can be tested.
```

## 3. Flight Data Normalization

```text
Design a Python function that converts raw aircraft state-vector responses into a
consistent flight-event format. Include useful fields such as the aircraft
identifier, callsign, origin country, longitude, latitude, altitude, velocity,
heading, vertical rate, and observation time when they are available. Handle null,
invalid, or missing values safely, and explain the chosen data types.
```

## 4. Kafka Producer

```text
Help me implement a Python Kafka producer that publishes normalized flight events
to a Kafka topic. Show how to configure the broker and topic, serialize messages as
JSON, choose a message key, handle delivery errors, and log producer activity.
Keep configuration outside the source code where practical and explain how to run it.
```

## 5. Kafka Consumer

```text
Help me implement a Python Kafka consumer for the flight-event topic. The consumer
should deserialize messages, validate the event structure, handle malformed messages,
and pass valid events to the processing and persistence layers. Explain consumer-group
configuration, offset handling, and how to recover from temporary broker failures.
```

## 6. Real-Time Flight Stream Processing

```text
Help me design a processing pipeline for streaming aircraft position events. Explain
how to validate incoming events, handle duplicate or out-of-order observations, and
maintain useful in-memory or persisted state for analysis. Keep the processing logic
modular so it can be unit tested independently of Kafka and the external API.
```

## 7. Congestion Detection

```text
Help me design a configurable method to detect aircraft congestion from flight
position data. Consider dividing a geographic area into grid cells and counting
aircraft within each cell over a rolling time window. Explain how to choose cell size
and thresholds, avoid counting stale observations, and generate a congestion event
when a threshold is exceeded. Include Python pseudocode or an implementation that
fits the project's data model.
```

## 8. SQLite Database Integration

```text
Help me design a SQLite schema for storing flight observations and congestion
alerts. Provide table definitions, suitable primary keys and indexes, and Python
functions for inserting and querying records. Consider duplicate events, timestamps,
transaction handling, and efficient queries for dashboard summaries.
```

## 9. Streamlit Dashboard

```text
Help me build a Streamlit dashboard for a real-time flight monitoring project.
Suggest a clear layout for key flight metrics, recent observations, aircraft density,
congestion alerts, and historical records. Show how to read data from SQLite, refresh
the display safely, handle empty results, and present charts or tables that are easy
for a user to understand.
```

## 10. Visualizations and Analytics

```text
Recommend useful visualizations for a flight streaming and congestion monitoring
dashboard. Include options for aircraft counts over time, geographic distribution,
congestion by grid cell, and recent alerts. For each visualization, specify the data
needed, an appropriate chart type, and how to handle missing or stale observations.
```

## 11. Error Handling and Logging

```text
Review the error-handling approach for this Python streaming application. Suggest
consistent logging and recovery strategies for API requests, Kafka producer and
consumer operations, data validation, and SQLite access. Distinguish recoverable
errors from fatal errors and avoid silently dropping important failures.
```

## 12. Testing

```text
Help me create pytest tests for the flight streaming system. Cover API response
normalization, missing and invalid fields, Kafka message serialization and
consumption, congestion detection, and SQLite persistence. Use fixtures and mocks
so that tests do not require live API calls or a running Kafka broker. Include edge
cases and explain how to run the tests.
```

## 13. Debugging and Code Review

```text
Review the following code from my flight streaming system. First explain what the
code currently does. Then identify bugs, edge cases, reliability problems, and
possible performance issues. Suggest the smallest practical changes, preserve the
existing project structure where possible, and provide an updated version with an
explanation of each important change.

[Paste the relevant code and error message here.]
```

## 14. Setup and Run Instructions

```text
Help me write clear setup and execution instructions for this project. Include
prerequisites, environment setup, dependency installation, configuration, starting
Kafka if required, launching the producer and consumer, initializing the database,
and starting the Streamlit dashboard. Use the actual filenames and commands from
my repository; do not invent files or commands that are not present.
```

## 15. README and Project Documentation

```text
Help me improve the README for my flight streaming system. Explain the project
purpose, architecture, technologies, data flow, major features, setup instructions,
execution steps, and testing. Keep all technical claims consistent with the actual
source code and repository. Mark any information that needs confirmation instead
of making assumptions.
```

## 16. Final Project Review

```text
Act as a software project reviewer. Evaluate this flight streaming system against
its stated objectives. Review architecture, data ingestion, Kafka streaming,
processing, congestion detection, persistence, dashboard usability, reliability,
testing, and documentation. Separate verified functionality from recommendations
and identify the highest-priority improvements.
```

---

## How to Keep This File Accurate

- Replace template wording with the exact prompts from your ChatGPT history when available.
- Remove sections for tasks that were not part of your project.
- Add the relevant project file or module next to each prompt if you want to show where it was applied.
- Do not include API keys, passwords, access tokens, or other secrets in this document.
- Update this file as the project evolves.
