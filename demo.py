#!/usr/bin/env python3
"""Tally Demo — WPM Typing Test (keystroke-level)

Every keystroke fires an event to Tally. Real-time features computed:
WPM, accuracy, keystroke rate, max gap between keys, error streaks.
"""

import sys
import os
import tty
import termios
import time

sys.path.insert(0, "python")

import tally as tl

# -- Tally v2.0 Pipeline: one event per keystroke ------------------

@tl.source
class RawKeystrokes:
    """Raw keystroke events from the typing test."""
    pass

@tl.dataset(depends_on=[RawKeystrokes])
class KeystrokeFeatures:
    features = tl.group_by("user_id").agg(
        keys_total=tl.count(window="1h"),
        keys_1m=tl.count(window="1m"),
        correct_1m=tl.count(window="1m", where="_event.correct == 1"),
        errors_1m=tl.count(window="1m", where="_event.correct == 0"),
        avg_gap_ms=tl.avg("gap_ms", window="1m"),
        max_gap_ms=tl.max("gap_ms", window="1m"),
    )
    accuracy = tl.derive("correct_1m / keys_1m * 100")
    wpm = tl.derive("keys_1m / 5")  # standard: 1 word = 5 chars

# -- Passages ------------------------------------------------------

PASSAGES = [
    "the quick brown fox jumps over the lazy dog",
    "push events get features no kafka no flink one binary",
    "real time streaming features with sub millisecond latency",
    "in memory state with periodic snapshots to local disk",
]

# -- Terminal helpers ----------------------------------------------

GRAY = "\033[90m"
GREEN = "\033[32m"
RED = "\033[31m"
BOLD = "\033[1m"
CYAN = "\033[36m"
DIM = "\033[2m"
RESET = "\033[0m"
CLEAR_LINE = "\033[2K\r"

def read_char():
    """Read a single character in raw mode."""
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
    return ch

def render_line(target, typed_chars):
    """Render the target text with typed chars colored green/red."""
    out = []
    for i, ch in enumerate(typed_chars):
        if ch == target[i]:
            out.append(f"{GREEN}{target[i]}{RESET}")
        else:
            out.append(f"{RED}{target[i]}{RESET}")
    # Cursor position (next char to type)
    if len(typed_chars) < len(target):
        out.append(f"\033[4m{target[len(typed_chars)]}\033[0m")  # underline cursor
        out.append(f"{GRAY}{target[len(typed_chars)+1:]}{RESET}")
    return "".join(out)

def render_stats(f, elapsed_sec):
    """Build stats line from Tally features."""
    wpm = f.wpm or 0
    # Scale for short sessions
    if elapsed_sec < 60 and elapsed_sec > 0:
        total = f.keys_total or 0
        wpm = int(total / 5 / (elapsed_sec / 60))

    acc = f.accuracy
    acc_str = f"{acc:.0f}%" if acc is not None else "--"
    avg_gap = f.avg_gap_ms
    avg_gap_str = f"{avg_gap:.0f}ms" if avg_gap is not None else "--"
    max_gap = f.max_gap_ms
    max_gap_str = f"{max_gap:.0f}ms" if max_gap is not None else "--"
    correct = f.correct_1m or 0
    errors = f.errors_1m or 0

    return (f"  {CYAN}WPM: {BOLD}{wpm}{RESET}  "
            f"accuracy: {acc_str}  "
            f"{GREEN}ok:{correct}{RESET} {RED}err:{errors}{RESET}  "
            f"avg gap: {avg_gap_str}  "
            f"max gap: {max_gap_str}")

# -- Main ----------------------------------------------------------

def main():
    print(f"\n{BOLD}  TALLY WPM DEMO{RESET}")
    print(f"  {GRAY}Every keystroke -> Tally event -> live features{RESET}")
    print(f"  {GRAY}Ctrl+C to quit | Backspace to correct{RESET}\n")

    app = tl.App("localhost:6400")
    app.register(RawKeystrokes, KeystrokeFeatures)

    user_id = "demo_user"
    start_time = time.time()
    last_key_time = start_time
    passage_idx = 0

    try:
        while passage_idx < len(PASSAGES):
            target = PASSAGES[passage_idx]
            typed_chars = []

            # Show passage header
            sys.stdout.write(f"  {DIM}passage {passage_idx + 1}/{len(PASSAGES)}{RESET}\n")
            sys.stdout.write(f"  {render_line(target, typed_chars)}\n")
            sys.stdout.write(f"\n")  # stats line placeholder
            sys.stdout.flush()

            while len(typed_chars) < len(target):
                ch = read_char()

                # Ctrl+C
                if ch == '\x03':
                    raise KeyboardInterrupt

                # Backspace
                if ch in ('\x7f', '\x08'):
                    if typed_chars:
                        typed_chars.pop()
                    # Re-render without pushing event
                    sys.stdout.write(f"\033[3A")  # move up 3 lines
                    sys.stdout.write(f"  {DIM}passage {passage_idx + 1}/{len(PASSAGES)}{RESET}\n")
                    sys.stdout.write(f"{CLEAR_LINE}  {render_line(target, typed_chars)}\n")
                    sys.stdout.write(f"\n")
                    sys.stdout.flush()
                    continue

                # Ignore non-printable
                if ord(ch) < 32:
                    continue

                now = time.time()
                gap_ms = int((now - last_key_time) * 1000)
                last_key_time = now

                pos = len(typed_chars)
                correct = 1 if ch == target[pos] else 0
                typed_chars.append(ch)

                elapsed = now - start_time

                # Push keystroke event to Tally
                features = app.push_sync(RawKeystrokes, {
                    "user_id": user_id,
                    "char": ch,
                    "expected": target[pos],
                    "correct": correct,
                    "gap_ms": gap_ms,
                    "position": pos,
                })

                # Re-render: move up 3 lines, redraw
                sys.stdout.write(f"\033[3A")  # up 3
                sys.stdout.write(f"  {DIM}passage {passage_idx + 1}/{len(PASSAGES)}{RESET}\n")
                sys.stdout.write(f"{CLEAR_LINE}  {render_line(target, typed_chars)}\n")
                sys.stdout.write(f"{CLEAR_LINE}{render_stats(features, elapsed)}\n")
                sys.stdout.flush()

            # Passage complete
            sys.stdout.write(f"\n  {GREEN}done!{RESET}\n\n")
            sys.stdout.flush()
            passage_idx += 1

        # All passages done
        elapsed = time.time() - start_time
        final = app.get(user_id)
        total = final._data.get("keys_total", 0)
        correct = final._data.get("correct_1m", 0)
        errors = final._data.get("errors_1m", 0)
        acc = final._data.get("accuracy")

        print(f"\n  {BOLD}ALL PASSAGES COMPLETE{RESET}")
        print(f"  {GRAY}Session: {elapsed:.0f}s{RESET}")
        if elapsed > 0 and total:
            print(f"  {BOLD}Final WPM: {int(total / 5 / (elapsed / 60))}{RESET}")
        if acc is not None:
            print(f"  Accuracy: {acc:.0f}%")
        print(f"  Keystrokes: {total}  (correct: {correct}, errors: {errors})")
        print()

    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        final = app.get(user_id)
        total = final._data.get("keys_total", 0)
        correct = final._data.get("correct_1m", 0)
        errors = final._data.get("errors_1m", 0)
        acc = final._data.get("accuracy")

        # Reset terminal
        sys.stdout.write("\n\n")
        print(f"  {GRAY}Session: {elapsed:.0f}s{RESET}")
        if elapsed > 0 and total:
            print(f"  {BOLD}Final WPM: {int(total / 5 / (elapsed / 60))}{RESET}")
        if acc is not None:
            print(f"  Accuracy: {acc:.0f}%")
        print(f"  Keystrokes: {total}  (correct: {correct}, errors: {errors})")
        print()

    finally:
        app.close()

if __name__ == "__main__":
    main()
