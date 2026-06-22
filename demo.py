"""
CPE 310 Group 7 — P2P File Transfer System: Live Simulation Demo
=================================================================
Run this script directly to see the full P2P system in action:
    python demo.py
"""

import hashlib
import random
import time

from src.chunk_data import ChunkData
from src.exceptions import ChecksumMismatchError, PeerNotFoundError
from src.file_metadata import FileMetadata
from src.nodes import DataPeerNode, MetadataTrackerNode, SeedNode
from src.protocols import PullProtocol, PushProtocol
from src.swarm import Swarm
from src.transfer_session import TransferSession

SEP  = "=" * 60
SEP2 = "-" * 60

def bar(pct, width=30):
    filled = int(width * pct / 100)
    return "[" + "#" * filled + "-" * (width - filled) + f"] {pct:.1f}%"

def section(title):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


# ──────────────────────────────────────────────────────────────
# SCENE 1 — Create a file and inspect its metadata
# ──────────────────────────────────────────────────────────────
section("SCENE 1: File Creation & Metadata")

FILE_CONTENT = (
    b"CPE 310 Group 7 - P2P File Transfer System\n"
    b"Federal University Oye-Ekiti (FUOYE)\n"
    b"This file is split into chunks and transferred across peers.\n"
    b"Each chunk is verified with SHA-256 to detect any corruption.\n"
) * 10   # ~1.6 KB

CHUNK_SIZE = 256

metadata = FileMetadata("lecture_notes.txt", FILE_CONTENT, chunk_size_bytes=CHUNK_SIZE)

print(f"  Filename   : {metadata.filename}")
print(f"  File size  : {metadata.size_bytes} bytes")
print(f"  Chunk size : {metadata.chunk_size_bytes} bytes")
print(f"  Chunks     : {metadata.chunk_count}")
print(f"  SHA-256    : {metadata.sha256_hash[:32]}...")


# ──────────────────────────────────────────────────────────────
# SCENE 2 — Seed node shares the file
# ──────────────────────────────────────────────────────────────
section("SCENE 2: Seed Node Shares File")

seed = SeedNode("seed-01", "192.168.1.1:9000", bandwidth=100.0, max_bandwidth_bps=500_000)
seed.share_file(FILE_CONTENT, "lecture_notes.txt", chunk_size=CHUNK_SIZE)

print(f"  Seed node  : {seed.node_id}  [{seed.address}]")
print(f"  Bandwidth  : {seed.bandwidth} Mbps (upload cap: {seed.max_bandwidth_bps} B/s)")
print(f"  Holding    : {len(seed.local_files)} file(s)")
print(f"  Has file?  : {seed.has_file(metadata.sha256_hash)}")


# ──────────────────────────────────────────────────────────────
# SCENE 3 — Tracker registers the file and peers
# ──────────────────────────────────────────────────────────────
section("SCENE 3: Tracker Registers Peers")

tracker = MetadataTrackerNode("tracker-01", "192.168.1.0:6969")

peer_specs = [
    ("peer-A", "192.168.1.10:8000", 80.0),
    ("peer-B", "192.168.1.11:8001", 45.0),
    ("peer-C", "192.168.1.12:8002", 30.0),
]

extra_peers = []
for pid, addr, bw in peer_specs:
    p = DataPeerNode(pid, addr, bandwidth=bw)
    p.share_file(FILE_CONTENT, "lecture_notes.txt", chunk_size=CHUNK_SIZE)
    tracker.register_file(metadata, p)
    extra_peers.append(p)
    print(f"  Registered : {pid}  [{addr}]  bandwidth={bw} Mbps")

tracker.register_file(metadata, seed)
print(f"  Registered : {seed.node_id}  [{seed.address}]  bandwidth={seed.bandwidth} Mbps")

ranked = tracker.find_peers(metadata.sha256_hash)
print(f"\n  Tracker peer ranking (fastest first):")
for i, p in enumerate(ranked, 1):
    print(f"    {i}. {p.node_id}  ({p.bandwidth} Mbps)")


# ──────────────────────────────────────────────────────────────
# SCENE 4 — New peer downloads via PushProtocol (no corruption)
# ──────────────────────────────────────────────────────────────
section("SCENE 4: Clean Download — PushProtocol (0% corruption)")

downloader = DataPeerNode("peer-NEW", "192.168.1.99:8080", bandwidth=60.0)
best_peer  = tracker.find_peers(metadata.sha256_hash)[0]
session    = TransferSession(downloader, best_peer, metadata)
proto      = PushProtocol(corruption_probability=0.0)

print(f"  Downloader : {downloader.node_id}")
print(f"  Provider   : {best_peer.node_id}")
print(f"  Protocol   : {proto}")
print()

proto.initiate(session)
for idx in range(metadata.chunk_count):
    chunk = proto.transfer_chunk(session, idx, provider=best_peer)
    session.mark_received(chunk)
    print(f"  Chunk {idx+1:>2}/{metadata.chunk_count}  size={len(chunk.data):>3}B  "
          f"checksum={chunk.checksum[:12]}...  verify={chunk.verify()}  "
          f"{bar(session.progress_pct)}")

ok = proto.finalise(session)
print(f"\n  Complete      : {session.is_complete()}")
print(f"  Whole-file SHA-256 verified : {ok}")
print(f"  Retries used  : {session.retry_count}")


# ──────────────────────────────────────────────────────────────
# SCENE 5 — Download with 40% simulated corruption + retries
# ──────────────────────────────────────────────────────────────
section("SCENE 5: Noisy Network — PullProtocol (40% corruption + retries)")

random.seed(7)

downloader2 = DataPeerNode("peer-STUDENT", "192.168.1.50:9090", bandwidth=55.0)
providers   = tracker.find_peers(metadata.sha256_hash)
session2    = TransferSession(downloader2, providers[0], metadata)
proto2      = PullProtocol(corruption_probability=0.4)

print(f"  Downloader : {downloader2.node_id}")
print(f"  Providers  : {[p.node_id for p in providers]}")
print(f"  Protocol   : {proto2}")
print()

proto2.initiate(session2)
total_retries = 0

for chunk_idx in range(metadata.chunk_count):
    attempt = 0
    for provider in providers * 50:
        try:
            chunk = proto2.transfer_chunk(session2, chunk_idx, provider=provider)
            session2.mark_received(chunk)
            status = "OK" if attempt == 0 else f"OK (after {attempt} retry)"
            print(f"  Chunk {chunk_idx+1:>2}/{metadata.chunk_count}  [{provider.node_id}]  "
                  f"{status:<20}  {bar(session2.progress_pct)}")
            break
        except ChecksumMismatchError as e:
            total_retries += 1
            attempt += 1
            session2.increment_retry()
            print(f"  Chunk {chunk_idx+1:>2}/{metadata.chunk_count}  [{provider.node_id}]  "
                  f"CORRUPTED! Retrying...  (expected={e.expected[:8]}... got={e.received[:8]}...)")

ok2 = proto2.finalise(session2)
print(f"\n  Complete      : {session2.is_complete()}")
print(f"  Whole-file SHA-256 verified : {ok2}")
print(f"  Total retries : {total_retries}")


# ──────────────────────────────────────────────────────────────
# SCENE 6 — Integrity check: tamper with a chunk
# ──────────────────────────────────────────────────────────────
section("SCENE 6: Integrity Verification — Tampered Chunk Detected")

original = ChunkData(metadata.sha256_hash, 0, FILE_CONTENT[:CHUNK_SIZE])
tampered = ChunkData._from_raw(
    metadata.sha256_hash, 0,
    b"\x00" * CHUNK_SIZE,
    original.checksum
)

print(f"  Original chunk[0]  : verify() = {original.verify()}  (data intact)")
print(f"  Tampered chunk[0]  : verify() = {tampered.verify()}  (data modified — checksum mismatch!)")
print(f"  Expected checksum  : {original.checksum[:32]}...")
print(f"  Actual data hash   : {hashlib.sha256(tampered.data).hexdigest()[:32]}...")


# ──────────────────────────────────────────────────────────────
# SCENE 7 — Swarm membership
# ──────────────────────────────────────────────────────────────
section("SCENE 7: Swarm Membership")

swarm = tracker.get_swarm(metadata.sha256_hash)
print(f"  Swarm size : {len(swarm)} peers")
print(f"  Members    :")
for p in swarm:
    print(f"    - {p.node_id}  [{p.address}]  {p.bandwidth} Mbps")

print(f"\n  'peer-A' in swarm? {'peer-A' in swarm}")
print(f"  'ghost'  in swarm? {'ghost'  in swarm}")


# ──────────────────────────────────────────────────────────────
# SCENE 8 — Rate limiter in action
# ──────────────────────────────────────────────────────────────
section("SCENE 8: Rate Limiter Simulation")

limited_seed = SeedNode("seed-limited", "10.0.0.1:9000", max_bandwidth_bps=500)
limited_seed.share_file(b"X" * 1000, "bigfile.bin", chunk_size=100)

fake_meta = FileMetadata("bigfile.bin", b"X" * 1000, 100)
print(f"  Seed upload cap : {limited_seed.max_bandwidth_bps} B/s")
print(f"  Trying to send chunks sequentially:\n")

for i in range(5):
    chunk = limited_seed.request_chunk(fake_meta.sha256_hash, i)
    can   = limited_seed.can_send(len(chunk.data))
    print(f"  Chunk[{i}]  size={len(chunk.data)}B  "
          f"bytes_used={limited_seed.bytes_sent_this_second}  "
          f"can_send_next={can}")
    if not can:
        print(f"  >>> Rate limit reached! Resetting counter (simulating next second)...")
        limited_seed.reset_bandwidth()


# ──────────────────────────────────────────────────────────────
# FINAL SUMMARY
# ──────────────────────────────────────────────────────────────
section("SIMULATION COMPLETE")
print(f"  Scene 1 : File metadata created & inspected         [DONE]")
print(f"  Scene 2 : Seed node shares file                     [DONE]")
print(f"  Scene 3 : Tracker registers {len(peer_specs)+1} peers, ranked by BW   [DONE]")
print(f"  Scene 4 : Clean PushProtocol download (0% noise)    [DONE]")
print(f"  Scene 5 : Noisy PullProtocol download (40% corrupt) [DONE]  retries={total_retries}")
print(f"  Scene 6 : Tampered chunk detected by verify()       [DONE]")
print(f"  Scene 7 : Swarm membership & lookup                 [DONE]")
print(f"  Scene 8 : Rate limiter enforced upload cap          [DONE]")
print(f"\n{SEP}")
