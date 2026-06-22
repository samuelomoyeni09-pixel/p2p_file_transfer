"""
Pytest test suite for the P2P File Transfer and Integrity Verification System.
Minimum 20 test cases covering all OOP concepts from Weeks 1–5.
"""

import hashlib
import pytest

from src.chunk_data import ChunkData
from src.exceptions import (
    ChecksumMismatchError,
    DuplicateFileError,
    P2PError,
    PeerNotFoundError,
    TransferTimeoutError,
)
from src.file_metadata import FileMetadata
from src.nodes import DataPeerNode, MetadataTrackerNode, RateLimiterMixin, SeedNode
from src.protocols import PullProtocol, PushProtocol
from src.swarm import Swarm
from src.transfer_session import TransferSession


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def sample_data():
    return b'Hello from Group 7! ' * 50   # 1000 bytes

@pytest.fixture
def small_data():
    return b'FUOYE'

@pytest.fixture
def seed(sample_data):
    s = SeedNode('seed-01', '10.0.0.1:8080', bandwidth=100.0)
    s.share_file(sample_data, 'test.txt', chunk_size=256)
    return s

@pytest.fixture
def metadata(sample_data):
    return FileMetadata('test.txt', sample_data, chunk_size_bytes=256)

@pytest.fixture
def tracker(seed, metadata):
    t = MetadataTrackerNode('tracker-01', '10.0.0.0:6969')
    t.register_file(metadata, seed)
    return t

@pytest.fixture
def peer():
    return DataPeerNode('peer-01', '10.0.0.2:8080', bandwidth=50.0)

@pytest.fixture
def session(peer, seed, metadata):
    return TransferSession(peer, seed, metadata)


# ============================================================
# 1. FileMetadata — constructor and properties
# ============================================================

def test_filemetadata_basic_properties(sample_data):
    m = FileMetadata('file.txt', sample_data, chunk_size_bytes=256)
    print(f"\n[FileMetadata] Created: {m}")
    print(f"  filename   = {m.filename}")
    print(f"  size_bytes = {m.size_bytes}")
    print(f"  sha256     = {m.sha256_hash[:16]}...")
    assert m.filename == 'file.txt'
    assert m.size_bytes == len(sample_data)
    assert m.chunk_size_bytes == 256
    assert m.sha256_hash == hashlib.sha256(sample_data).hexdigest()

def test_filemetadata_chunk_count_exact():
    data = b'A' * 512   # exactly 2 chunks of 256
    m = FileMetadata('f', data, chunk_size_bytes=256)
    print(f"\n[FileMetadata] 512 bytes / 256 chunk_size -> chunk_count = {m.chunk_count}")
    assert m.chunk_count == 2

def test_filemetadata_chunk_count_remainder():
    data = b'A' * 300   # 2 chunks: 256 + 44
    m = FileMetadata('f', data, chunk_size_bytes=256)
    print(f"\n[FileMetadata] 300 bytes / 256 chunk_size -> chunk_count = {m.chunk_count} (last chunk = 44 bytes)")
    assert m.chunk_count == 2

def test_filemetadata_invalid_chunk_size():
    print("\n[FileMetadata] Attempting chunk_size=0 -> expecting ValueError...")
    with pytest.raises(ValueError):
        FileMetadata('f', b'hello', chunk_size_bytes=0)
    print("  ValueError raised correctly.")

def test_filemetadata_equality_same_content(sample_data):
    m1 = FileMetadata('a.txt', sample_data, 256)
    m2 = FileMetadata('b.txt', sample_data, 256)   # different name, same bytes
    print(f"\n[FileMetadata] 'a.txt' == 'b.txt' (same content)? {m1 == m2}")
    assert m1 == m2

def test_filemetadata_inequality_different_content():
    m1 = FileMetadata('f', b'hello', 256)
    m2 = FileMetadata('f', b'world', 256)
    print(f"\n[FileMetadata] b'hello' != b'world'? {m1 != m2}")
    assert m1 != m2

def test_filemetadata_ordering():
    small = FileMetadata('s', b'hi', 256)
    large = FileMetadata('l', b'hello world!', 256)
    print(f"\n[FileMetadata] Ordering: small({small.size_bytes}B) < large({large.size_bytes}B)? {small < large}")
    assert small < large
    assert not large < small

def test_filemetadata_hash_consistency(sample_data):
    m1 = FileMetadata('f', sample_data, 256)
    m2 = FileMetadata('f', sample_data, 256)
    print(f"\n[FileMetadata] hash(m1) == hash(m2)? {hash(m1) == hash(m2)}")
    assert hash(m1) == hash(m2)

def test_filemetadata_str_repr(sample_data):
    m = FileMetadata('notes.txt', sample_data, 256)
    print(f"\n[FileMetadata] str()  -> {str(m)}")
    print(f"[FileMetadata] repr() -> {repr(m)}")
    assert 'notes.txt' in str(m)
    assert 'FileMetadata' in repr(m)


# ============================================================
# 2. ChunkData — immutability and verify()
# ============================================================

def test_chunkdata_verify_clean():
    data = b'clean data chunk'
    c = ChunkData('abc123', 0, data)
    print(f"\n[ChunkData] Clean chunk[0]: checksum={c.checksum[:16]}... verify()={c.verify()}")
    assert c.verify() is True

def test_chunkdata_verify_corrupted():
    data = b'clean data chunk'
    c = ChunkData('abc123', 0, data)
    corrupted = ChunkData._from_raw('abc123', 0, b'dirty data chunk', c.checksum)
    print(f"\n[ChunkData] Corrupted chunk[0]:")
    print(f"  Original checksum : {c.checksum[:16]}...")
    print(f"  Corrupted data    : b'dirty data chunk'")
    print(f"  verify()          = {corrupted.verify()}  <- checksum mismatch detected!")
    assert corrupted.verify() is False

def test_chunkdata_properties():
    data = b'chunk content'
    c = ChunkData('filehash', 3, data)
    print(f"\n[ChunkData] chunk[3]: file_hash='filehash', data={c.data!r}, checksum={c.checksum[:16]}...")
    assert c.file_hash == 'filehash'
    assert c.chunk_index == 3
    assert c.data == data
    assert c.checksum == hashlib.sha256(data).hexdigest()

def test_chunkdata_invalid_index():
    print("\n[ChunkData] Creating chunk with index=-1 -> expecting ValueError...")
    with pytest.raises(ValueError):
        ChunkData('hash', -1, b'data')
    print("  ValueError raised correctly.")

def test_chunkdata_str_and_repr():
    c = ChunkData('myhash', 0, b'test')
    print(f"\n[ChunkData] str()  -> {str(c)}")
    print(f"[ChunkData] repr() -> {repr(c)}")
    assert 'ChunkData' in str(c)
    assert 'ChunkData' in repr(c)


# ============================================================
# 3. Swarm — __len__, __contains__, __iter__
# ============================================================

def test_swarm_add_and_len():
    swarm = Swarm('filehash')
    peer = DataPeerNode('p1', 'addr')
    swarm.add_peer(peer)
    print(f"\n[Swarm] Added peer 'p1' -> len(swarm) = {len(swarm)}")
    assert len(swarm) == 1

def test_swarm_contains_by_id():
    swarm = Swarm('filehash')
    peer = DataPeerNode('p-unique', 'addr')
    swarm.add_peer(peer)
    print(f"\n[Swarm] 'p-unique' in swarm? {('p-unique' in swarm)}")
    print(f"[Swarm] 'unknown'  in swarm? {('unknown'  in swarm)}")
    assert 'p-unique' in swarm
    assert 'unknown' not in swarm

def test_swarm_contains_by_object():
    swarm = Swarm('fh')
    peer = DataPeerNode('p1', 'addr')
    swarm.add_peer(peer)
    print(f"\n[Swarm] peer object in swarm? {peer in swarm}")
    assert peer in swarm

def test_swarm_remove_peer():
    swarm = Swarm('fh')
    peer = DataPeerNode('p1', 'addr')
    swarm.add_peer(peer)
    swarm.remove_peer(peer)
    print(f"\n[Swarm] After removing 'p1' -> len(swarm) = {len(swarm)}")
    assert len(swarm) == 0

def test_swarm_iteration():
    swarm = Swarm('fh')
    peers = [DataPeerNode(f'p{i}', f'addr{i}') for i in range(3)]
    for p in peers:
        swarm.add_peer(p)
    ids = {p.node_id for p in swarm}
    print(f"\n[Swarm] Iterating 3-peer swarm -> peer IDs = {sorted(ids)}")
    assert ids == {'p0', 'p1', 'p2'}


# ============================================================
# 4. DataPeerNode and MetadataTrackerNode
# ============================================================

def test_peer_share_file(sample_data):
    peer = DataPeerNode('p', 'addr')
    meta = peer.share_file(sample_data, 'file.txt', chunk_size=256)
    print(f"\n[DataPeerNode] 'p' shared 'file.txt' ({meta.size_bytes}B, {meta.chunk_count} chunks)")
    print(f"  has_file(sha256)? {peer.has_file(meta.sha256_hash)}")
    assert peer.has_file(meta.sha256_hash)

def test_peer_request_chunk(sample_data):
    peer = DataPeerNode('p', 'addr')
    meta = peer.share_file(sample_data, 'file.txt', chunk_size=256)
    chunk = peer.request_chunk(meta.sha256_hash, 0)
    print(f"\n[DataPeerNode] Requested chunk[0]: index={chunk.chunk_index}, "
          f"size={len(chunk.data)}B, verify()={chunk.verify()}")
    assert chunk.chunk_index == 0
    assert chunk.verify()

def test_peer_duplicate_file_error(sample_data):
    peer = DataPeerNode('p', 'addr')
    peer.share_file(sample_data, 'file.txt', chunk_size=256)
    print("\n[DataPeerNode] Sharing same file again -> expecting DuplicateFileError...")
    with pytest.raises(DuplicateFileError):
        peer.share_file(sample_data, 'file.txt', chunk_size=256)
    print("  DuplicateFileError raised correctly.")

def test_tracker_find_peers_sorted_by_bandwidth(metadata):
    tracker = MetadataTrackerNode('t', 'addr')
    fast = DataPeerNode('fast', 'a', bandwidth=90.0)
    slow = DataPeerNode('slow', 'b', bandwidth=10.0)
    tracker.register_file(metadata, slow)
    tracker.register_file(metadata, fast)
    peers = tracker.find_peers(metadata.sha256_hash)
    print(f"\n[Tracker] Peers sorted by bandwidth (highest first):")
    for i, p in enumerate(peers):
        print(f"  [{i}] {p.node_id} ({p.bandwidth} Mbps)")
    assert peers[0].node_id == 'fast'

def test_tracker_no_peers_raises(metadata):
    tracker = MetadataTrackerNode('t', 'addr')
    print("\n[Tracker] Querying unknown file hash -> expecting PeerNotFoundError...")
    with pytest.raises(PeerNotFoundError):
        tracker.find_peers(metadata.sha256_hash)
    print("  PeerNotFoundError raised correctly.")

def test_tracker_find_peers_limit(metadata):
    tracker = MetadataTrackerNode('t', 'addr')
    for i in range(10):
        p = DataPeerNode(f'p{i}', f'addr{i}', bandwidth=float(i))
        tracker.register_file(metadata, p)
    peers = tracker.find_peers(metadata.sha256_hash, limit=3)
    print(f"\n[Tracker] 10 peers registered; find_peers(limit=3) returned {len(peers)} peer(s): "
          f"{[p.node_id for p in peers]}")
    assert len(peers) == 3


# ============================================================
# 5. TransferSession — progress and completion
# ============================================================

def test_session_initial_progress(session, metadata):
    print(f"\n[TransferSession] Initial state: {session}")
    print(f"  progress  = {session.progress_pct:.1f}%")
    print(f"  complete  = {session.is_complete()}")
    print(f"  pending   = {len(session.pending_chunks)} chunks")
    assert session.progress_pct == 0.0
    assert not session.is_complete()
    assert len(session.pending_chunks) == metadata.chunk_count

def test_session_mark_received_updates_progress(session, seed, metadata):
    chunk = seed.request_chunk(metadata.sha256_hash, 0)
    session.mark_received(chunk)
    total = metadata.chunk_count
    expected_pct = (1 / total) * 100.0
    print(f"\n[TransferSession] Received chunk[0] of {total}: progress = {session.progress_pct:.1f}%")
    assert abs(session.progress_pct - expected_pct) < 0.01

def test_session_is_complete(seed, metadata):
    peer = DataPeerNode('p', 'addr')
    session = TransferSession(peer, seed, metadata)
    print(f"\n[TransferSession] Downloading {metadata.chunk_count} chunks from seed '{seed.node_id}':")
    for i in range(metadata.chunk_count):
        chunk = seed.request_chunk(metadata.sha256_hash, i)
        session.mark_received(chunk)
        print(f"  chunk[{i}] received -> progress {session.progress_pct:.1f}%")
    print(f"  complete = {session.is_complete()}")
    assert session.is_complete()
    assert session.progress_pct == 100.0

def test_session_str_repr(session):
    print(f"\n[TransferSession] str()  -> {str(session)}")
    print(f"[TransferSession] repr() -> {repr(session)}")
    assert 'TransferSession' in str(session)
    assert 'TransferSession' in repr(session)


# ============================================================
# 6. Protocols — clean transfer and corruption detection
# ============================================================

def test_push_protocol_clean_transfer(session, seed, metadata):
    proto = PushProtocol(corruption_probability=0.0)
    print(f"\n[PushProtocol] {proto} — transferring {metadata.chunk_count} chunks (0% corruption):")
    proto.initiate(session)
    for idx in range(metadata.chunk_count):
        chunk = proto.transfer_chunk(session, idx, provider=seed)
        session.mark_received(chunk)
        print(f"  chunk[{idx}] OK  verify={chunk.verify()}  progress={session.progress_pct:.1f}%")
    result = proto.finalise(session)
    print(f"  finalise() whole-file hash match = {result}")
    assert result

def test_pull_protocol_clean_transfer(session, seed, metadata):
    proto = PullProtocol(corruption_probability=0.0)
    print(f"\n[PullProtocol] {proto} — transferring {metadata.chunk_count} chunks (0% corruption):")
    proto.initiate(session)
    for idx in range(metadata.chunk_count):
        chunk = proto.transfer_chunk(session, idx, provider=seed)
        session.mark_received(chunk)
        print(f"  chunk[{idx}] OK  verify={chunk.verify()}  progress={session.progress_pct:.1f}%")
    result = proto.finalise(session)
    print(f"  finalise() whole-file hash match = {result}")
    assert result

def test_push_protocol_raises_on_corruption(seed, metadata):
    """With 100% corruption every chunk raises ChecksumMismatchError."""
    peer = DataPeerNode('p', 'addr')
    session = TransferSession(peer, seed, metadata)
    proto = PushProtocol(corruption_probability=1.0)
    proto.initiate(session)
    print(f"\n[PushProtocol] {proto} — requesting chunk[0] with 100% corruption -> expecting ChecksumMismatchError...")
    with pytest.raises(ChecksumMismatchError) as exc_info:
        proto.transfer_chunk(session, 0, provider=seed)
    print(f"  ChecksumMismatchError caught: {exc_info.value}")

def test_pull_protocol_raises_on_corruption(seed, metadata):
    peer = DataPeerNode('p', 'addr')
    session = TransferSession(peer, seed, metadata)
    proto = PullProtocol(corruption_probability=1.0)
    print(f"\n[PullProtocol] {proto} — requesting chunk[0] with 100% corruption -> expecting ChecksumMismatchError...")
    with pytest.raises(ChecksumMismatchError) as exc_info:
        proto.transfer_chunk(session, 0, provider=seed)
    print(f"  ChecksumMismatchError caught: {exc_info.value}")

def test_protocol_invalid_corruption_probability():
    print("\n[Protocol] Setting corruption_probability=1.5 -> expecting ValueError...")
    with pytest.raises(ValueError):
        PushProtocol(corruption_probability=1.5)
    print("  ValueError raised correctly.")

def test_protocol_str_repr():
    p = PushProtocol(corruption_probability=0.1)
    print(f"\n[Protocol] str()  -> {str(p)}")
    print(f"[Protocol] repr() -> {repr(p)}")
    assert 'PushProtocol' in str(p)
    assert 'PushProtocol' in repr(p)


# ============================================================
# 7. Custom exceptions — structured fields
# ============================================================

def test_checksum_mismatch_error_fields():
    err = ChecksumMismatchError('abcdef1234', 2, 'expected_hash', 'actual_hash')
    print(f"\n[Exception] ChecksumMismatchError: {err}")
    print(f"  file_hash={err.file_hash}, chunk_idx={err.chunk_idx}")
    print(f"  expected={err.expected!r}, received={err.received!r}")
    print(f"  isinstance(P2PError)={isinstance(err, P2PError)}, isinstance(IOError)={isinstance(err, IOError)}")
    assert err.file_hash == 'abcdef1234'
    assert err.chunk_idx == 2
    assert err.expected == 'expected_hash'
    assert err.received == 'actual_hash'
    assert isinstance(err, P2PError)
    assert isinstance(err, IOError)

def test_peer_not_found_error_fields():
    err = PeerNotFoundError('deadbeef', 'no peers')
    print(f"\n[Exception] PeerNotFoundError: {err}")
    print(f"  file_hash={err.file_hash}, isinstance(P2PError)={isinstance(err, P2PError)}")
    assert err.file_hash == 'deadbeef'
    assert 'deadbeef' in str(err)
    assert isinstance(err, P2PError)

def test_duplicate_file_error_fields():
    err = DuplicateFileError('aabbcc', 'myfile.txt')
    print(f"\n[Exception] DuplicateFileError: {err}")
    print(f"  file_hash={err.file_hash}, filename={err.filename}, isinstance(P2PError)={isinstance(err, P2PError)}")
    assert err.file_hash == 'aabbcc'
    assert err.filename == 'myfile.txt'
    assert isinstance(err, P2PError)

def test_transfer_timeout_error_fields():
    err = TransferTimeoutError('session-uuid-123', 5)
    print(f"\n[Exception] TransferTimeoutError: {err}")
    print(f"  session_id={err.session_id}, chunk_idx={err.chunk_idx}, isinstance(P2PError)={isinstance(err, P2PError)}")
    assert err.session_id == 'session-uuid-123'
    assert err.chunk_idx == 5
    assert isinstance(err, P2PError)


# ============================================================
# 8. SeedNode — multiple inheritance verification
# ============================================================

def test_seed_node_is_data_peer_and_rate_limiter():
    seed = SeedNode('s', 'addr')
    print(f"\n[SeedNode] isinstance(DataPeerNode)   = {isinstance(seed, DataPeerNode)}")
    print(f"[SeedNode] isinstance(RateLimiterMixin) = {isinstance(seed, RateLimiterMixin)}")
    assert isinstance(seed, DataPeerNode)
    assert isinstance(seed, RateLimiterMixin)

def test_seed_node_bandwidth_tracking(small_data):
    seed = SeedNode('s', 'addr', max_bandwidth_bps=100)
    seed.share_file(small_data, 'tiny.txt', chunk_size=256)
    meta = FileMetadata('tiny.txt', small_data, 256)
    seed.request_chunk(meta.sha256_hash, 0)
    print(f"\n[SeedNode] After serving one chunk: bytes_sent_this_second = {seed.bytes_sent_this_second}")
    assert seed.bytes_sent_this_second > 0

def test_rate_limiter_can_send():
    seed = SeedNode('s', 'addr', max_bandwidth_bps=1000)
    print(f"\n[RateLimiter] max_bandwidth=1000 bps")
    print(f"  can_send(500) before any transfer? {seed.can_send(500)}  (0 used so far)")
    assert seed.can_send(500)
    seed.record_sent(800)
    print(f"  After recording 800B sent:")
    print(f"  can_send(500)?  {seed.can_send(500)}  (800+500=1300 > 1000 -> rate limit hit)")
    assert not seed.can_send(500)   # 800 + 500 > 1000


# ============================================================
# 9. Polymorphism — duck typing
# ============================================================

def test_duck_typed_transfer(seed, metadata):
    """Any object with request_chunk() can serve as a provider."""
    def fetch_all(provider, file_hash, chunk_count):
        return [provider.request_chunk(file_hash, i) for i in range(chunk_count)]

    print(f"\n[Polymorphism] fetch_all() using duck-typed provider '{seed.node_id}':")
    chunks = fetch_all(seed, metadata.sha256_hash, metadata.chunk_count)
    for c in chunks:
        print(f"  chunk[{c.chunk_index}] size={len(c.data)}B  verify={c.verify()}")
    assert len(chunks) == metadata.chunk_count
    assert all(c.verify() for c in chunks)

def test_sorted_filemetadata():
    files = [
        FileMetadata('c', b'c' * 300, 256),
        FileMetadata('a', b'a' * 100, 256),
        FileMetadata('b', b'b' * 200, 256),
    ]
    ordered = sorted(files)
    print(f"\n[Polymorphism] sorted(FileMetadata list) by size -> {[f.filename for f in ordered]}")
    assert [f.filename for f in ordered] == ['a', 'b', 'c']


# ============================================================
# 10. Full integration — download with retries
# ============================================================

def test_full_transfer_with_corruption(sample_data):
    """End-to-end: seeder -> peer with 50% corruption, expect success via retries."""
    import random
    random.seed(42)   # deterministic outcome for the test

    seed = SeedNode('seed', '0.0.0.1:9000', bandwidth=100.0)
    meta = seed.share_file(sample_data, 'sample.txt', chunk_size=256)

    tracker = MetadataTrackerNode('tracker', '0.0.0.0:6969')
    tracker.register_file(meta, seed)

    peer = DataPeerNode('peer', '0.0.0.2:9000', bandwidth=50.0)
    proto = PushProtocol(corruption_probability=0.5)

    print(f"\n[Integration] End-to-end transfer with 50% simulated corruption")
    print(f"  File  : {meta.filename} ({meta.size_bytes}B, {meta.chunk_count} chunks of {meta.chunk_size_bytes}B)")
    print(f"  Seeder: {seed.node_id}  ({seed.bandwidth} Mbps)")
    print(f"  Peer  : {peer.node_id}  ({peer.bandwidth} Mbps)")
    print(f"  Proto : {proto}")

    from tests.test_p2p import _run_transfer
    session, retries, ok = _run_transfer(peer, tracker, meta, proto, verbose=True)

    print(f"\n  --- Transfer Summary ---")
    print(f"  Complete      : {session.is_complete()}")
    print(f"  Progress      : {session.progress_pct:.1f}%")
    print(f"  Total retries : {retries} (due to checksum failures)")
    print(f"  Whole-file SHA-256 match: {ok}")

    assert ok, 'Whole-file hash must verify after retried transfer'
    assert session.is_complete()


def _run_transfer(requester, tracker, metadata, protocol, max_retries=200, verbose=False):
    """Internal helper used by the integration test."""
    from src.exceptions import ChecksumMismatchError, PeerNotFoundError
    peers = tracker.find_peers(metadata.sha256_hash)
    session = TransferSession(requester, peers[0], metadata)
    protocol.initiate(session)
    total_retries = 0

    for chunk_idx in range(metadata.chunk_count):
        attempt = 0
        for peer in peers * max_retries:
            try:
                chunk = protocol.transfer_chunk(session, chunk_idx, provider=peer)
                session.mark_received(chunk)
                if verbose:
                    status = "OK" if attempt == 0 else f"OK after {attempt} retry(ies)"
                    print(f"  chunk[{chunk_idx}/{metadata.chunk_count - 1}] {status}  "
                          f"progress={session.progress_pct:.1f}%")
                break
            except ChecksumMismatchError:
                total_retries += 1
                attempt += 1
                session.increment_retry()
                if verbose:
                    print(f"  chunk[{chunk_idx}] CORRUPT — retry #{attempt}")

    return session, total_retries, protocol.finalise(session)
