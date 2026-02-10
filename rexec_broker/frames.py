import logging


def format_identity(frame: bytes) -> str:
    """
    Logging helper: Format a ZMQ identity frame for logging.
    """
    data = bytes(frame)
    if not data:
        return "<empty>"
    return f"{data.hex()}"


def split_envelope(frames):
    """
    Logging helper: Split a list of ZMQ frames into envelope and body.
    """
    for idx, frame in enumerate(frames):
        if frame == b"":
            envelope = frames[:idx]
            body = frames[idx + 1:]
            return envelope, idx, body
    return [], None, frames


def format_frames(frames, max_bytes=256, prefix="  "):
    """
    Logging helper: Format a list of ZMQ raw frames for logging.
    """
    lines = []
    for idx, frame in enumerate(frames):
        data = bytes(frame)
        if len(data) > max_bytes:
            preview = data[:max_bytes]
            lines.append(
                f"[{idx}] len={len(data)} data={preview!r}...<truncated>"
            )
        else:
            lines.append(f"[{idx}] len={len(data)} data={data!r}")
    return "\n".join(f"{prefix}{line}" for line in lines)


def log_routing_envelope(direction, frames, from_addr, to_addr, server_id=None):
    """
    Log the routing envelope of a message.
    """
    envelope, delimiter_index, body = split_envelope(frames)
    client_id = format_identity(envelope[0]) if envelope else "<unknown>"
    server_label = server_id if server_id else "<unknown>"

    # if no delimiter, log "unknown" envelope
    if delimiter_index is None:
        logging.info(
            "  %s | client=%s | server=%s | from=%s to=%s | routing envelope: <unknown> (no empty delimiter)",
            direction,
            client_id,
            server_label,
            from_addr,
            to_addr,
        )
    # if delimiter found, log envelope identities and body sizes
    else:
        envelope_ids = [format_identity(frame) for frame in envelope]
        body_sizes = [len(frame) for frame in body]
        logging.info(
            "  %s | client=%s | server=%s | from=%s to=%s | routing envelope: %s (delimiter frame %d, body frames=%d, body sizes=%s)",
            direction,
            client_id,
            server_label,
            from_addr,
            to_addr,
            envelope_ids,
            delimiter_index,
            len(body),
            body_sizes,
        )
    # Detailed frames logging if run in debug mode
    logging.debug(
        " %s | client=%s | server=%s | from=%s to=%s | raw frames:\n%s",
        direction,
        client_id,
        server_label,
        from_addr,
        to_addr,
        format_frames(frames),
    )
