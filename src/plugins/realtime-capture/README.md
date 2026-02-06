# Real-time Capture Plugin

Provides real-time monitoring of OpenCode events and forwards new message events to OpenPlanner.

## Features

- **Real-time Event Capture**: Observes OpenCode events as they happen
- **OpenPlanner Forwarding**: Streams `message.updated` and `message.removed` events to OpenPlanner through the gateway endpoint
- **Session Monitoring**: Track session updates and changes
- **Message Tracking**: Monitor message creation and updates
- **Filtering Options**: Filter events by type, session ID, etc.
- **Memory Management**: Automatically limits captured events to prevent memory issues

## Tools

### `start-realtime-capture`

Starts real-time capture of OpenCode events.

```bash
start-realtime-capture
```

### `stop-realtime-capture`

Stops real-time capture and provides a summary.

```bash
stop-realtime-capture
```

### `get-captured-events`

Retrieves recently captured events with filtering options.

```bash
get-captured-events --limit=50 --eventType="message.updated" --format="table"
```

### `get-capture-status`

Gets the current status of real-time capture.

```bash
get-capture-status
```

### `clear-captured-events`

Clears all captured events from memory.

```bash
clear-captured-events
```

### `get-active-sessions-realtime`

Gets current active sessions with real-time status.

```bash
get-active-sessions-realtime --includeMessages=true
```

## Usage Example

1. Start capturing events:

   ```bash
   start-realtime-capture
   ```

2. Do some work in OpenCode sessions

3. Check what was captured:

   ```bash
   get-captured-events --limit=10 --format="table"
   ```

4. Stop capturing and get summary:
   ```bash
   stop-realtime-capture
   ```

## Event Types

The plugin captures all OpenCode event types including:

- `message.updated` - Message updates
- `message.removed` - Message deletions
- `message.part.updated` - Message part updates
- `session.updated` - Session updates
- `session.idle` - Session idle events
- `session.compacted` - Session compaction
- `permission.updated` - Permission changes
- `file.edited` - File edits
- `server.connected` - Server connections
- And more...

## Memory Management

The plugin automatically limits the number of captured events to prevent memory issues:

- Maximum events stored: 1000
- Events are automatically trimmed to keep only the most recent
- Use `clear-captured-events` to manually clear memory

## Integration with Indexer

This plugin is responsible for real-time message ingestion. The `opencode-indexer` service should focus on historical backfill scans.

## Environment Variables

- `OPENPLANNER_EVENTS_ENABLED` (default: `1`) - enables/disables forwarding
- `OPENPLANNER_EVENTS_ENDPOINT` (default: `http://127.0.0.1:8788/api/openplanner/v1/events`) - gateway ingestion endpoint
- `OPENPLANNER_EVENTS_AUTH_TOKEN` (optional) - bearer token used when gateway requires auth
- `OPENPLANNER_EVENTS_SOURCE` (default: `opencode.realtime-capture`) - `source` value stored in OpenPlanner events
