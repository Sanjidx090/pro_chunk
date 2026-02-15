!wget -O videos_with_bangla.csv https://raw.githubusercontent.com/Sanjidx090/yt_chunks/main/videos_with_bangla.csv
#!/usr/bin/env python3
"""
SMART CHUNK-WISE BANGLA TRANSCRIPT DOWNLOADER
Random 20-30 second chunks that respect word boundaries
"""

!pip -q install youtube-transcript-api pandas
import pandas as pd
import time
import random
import os
import json
from youtube_transcript_api import YouTubeTranscriptApi
import youtube_transcript_api._errors as yt_errors

# ==============================
# CONFIG - EDIT THESE
# ==============================
INPUT_CSV = "videos_with_bangla.csv"
OUTPUT_DIR = "bangla_transcripts_16"
VIDEO_ID_COLUMN = "video_id"
# Batch processing
START_INDEX = 262
BATCH_SIZE = 20
# Smart chunking config
MIN_CHUNK_DURATION = 18  # Minimum chunk length in seconds
MAX_CHUNK_DURATION = 30  # Maximum chunk length in seconds



# Safety
MIN_WAIT = 2.0
MAX_WAIT = 4.0
SAVE_EVERY = 5

RESUME = True

# ==============================
# SETUP
# ==============================
os.makedirs(OUTPUT_DIR, exist_ok=True)
progress_file = os.path.join(OUTPUT_DIR, "download_progress.json")

print("=" * 70)
print("SMART BANGLA TRANSCRIPT DOWNLOADER")
print("=" * 70)
print(f"Chunk size: {MIN_CHUNK_DURATION}-{MAX_CHUNK_DURATION}s (random, respects words)")
print(f"Output: {OUTPUT_DIR}/")
print()

# ==============================
# LOAD VIDEO LIST
# ==============================
df = pd.read_csv(INPUT_CSV)
all_video_ids = df[VIDEO_ID_COLUMN].dropna().astype(str).unique().tolist()
print(f"📌 Total Bangla videos: {len(all_video_ids)}")

# Check progress
downloaded = set()
if RESUME and os.path.exists(progress_file):
    with open(progress_file, 'r') as f:
        progress = json.load(f)
        downloaded = set(progress.get('completed', []))
    print(f"✅ Already downloaded: {len(downloaded)} videos")

# Filter and batch
remaining_videos = [vid for vid in all_video_ids if vid not in downloaded]
batch_videos = remaining_videos[START_INDEX:START_INDEX + BATCH_SIZE]

if len(batch_videos) == 0:
    print("\n✅ All videos already downloaded!")
    exit(0)

print(f"📝 Will download: {len(batch_videos)} videos in this batch")
print()

# ==============================
# API INSTANCE
# ==============================
api = YouTubeTranscriptApi()

# ==============================
# SMART CHUNKING FUNCTION
# ==============================

def chunk_smart_duration(segments, min_duration=20, max_duration=30):
    """
    Create chunks with random duration between min and max,
    respecting word/sentence boundaries (no mid-word cuts)

    Algorithm:
    1. Pick a random target duration (20-30s)
    2. Add segments until we reach/exceed target
    3. Stop at natural segment boundary
    4. Repeat for next chunk
    """
    if not segments:
        return []

    chunks = []
    chunk_id = 0
    i = 0

    while i < len(segments):
        # Pick random target duration for this chunk
        target_duration = random.uniform(min_duration, max_duration)

        # Start new chunk
        chunk_start = segments[i].start
        chunk_segments = []
        chunk_texts = []
        current_duration = 0

        # Add segments until we meet/exceed target duration
        while i < len(segments):
            seg = segments[i]
            seg_duration = seg.duration

            # Always include at least one segment (avoid empty chunks)
            if not chunk_segments:
                chunk_segments.append(seg)
                chunk_texts.append(seg.text)
                current_duration += seg_duration
                i += 1
                continue

            # Check if adding this segment would keep us near target
            new_duration = current_duration + seg_duration

            # If we haven't reached minimum yet, keep adding
            if current_duration < min_duration:
                chunk_segments.append(seg)
                chunk_texts.append(seg.text)
                current_duration += seg_duration
                i += 1
                continue

            # If we're past minimum, check if we should stop
            # Stop if:
            # 1. We're past target duration, OR
            # 2. Adding next segment would exceed max duration
            if current_duration >= target_duration or new_duration > max_duration:
                break

            # Otherwise, add this segment and continue
            chunk_segments.append(seg)
            chunk_texts.append(seg.text)
            current_duration += seg_duration
            i += 1

        # Create chunk
        if chunk_segments:
            chunk_end = chunk_segments[-1].start + chunk_segments[-1].duration
            chunks.append({
                'chunk_id': chunk_id,
                'start': chunk_start,
                'end': chunk_end,
                'duration': chunk_end - chunk_start,
                'text': " ".join(chunk_texts),
                'segments': len(chunk_segments),
                'target_duration': target_duration  # For analysis
            })
            chunk_id += 1

    return chunks

# ==============================
# DOWNLOAD FUNCTION
# ==============================

def download_transcript_chunks(video_id):
    """Download and chunk transcript for a video"""
    try:
        # Fetch Bangla transcript
        transcript = api.fetch(video_id, languages=['bn'])
        segments = list(transcript)

        if not segments:
            return {
                'success': False,
                'error': 'No segments found',
                'chunks': []
            }

        # Apply smart chunking
        chunks = chunk_smart_duration(
            segments,
            min_duration=MIN_CHUNK_DURATION,
            max_duration=MAX_CHUNK_DURATION
        )

        # Save chunks to files
        video_dir = os.path.join(OUTPUT_DIR, video_id)
        os.makedirs(video_dir, exist_ok=True)

        # Calculate statistics
        chunk_durations = [c['duration'] for c in chunks]
        avg_duration = sum(chunk_durations) / len(chunk_durations) if chunk_durations else 0

        # Save metadata
        metadata = {
            'video_id': video_id,
            'url': f'https://www.youtube.com/watch?v={video_id}',
            'total_chunks': len(chunks),
            'chunk_mode': 'smart_duration',
            'min_chunk_duration': MIN_CHUNK_DURATION,
            'max_chunk_duration': MAX_CHUNK_DURATION,
            'avg_chunk_duration': avg_duration,
            'total_duration': segments[-1].start + segments[-1].duration,
            'total_segments': len(segments)
        }

        with open(os.path.join(video_dir, 'metadata.json'), 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

        # Save each chunk
        for chunk in chunks:
            chunk_file = os.path.join(video_dir, f'chunk_{chunk["chunk_id"]:04d}.json')
            with open(chunk_file, 'w', encoding='utf-8') as f:
                json.dump(chunk, f, ensure_ascii=False, indent=2)

            # Also save as plain text
            txt_file = os.path.join(video_dir, f'chunk_{chunk["chunk_id"]:04d}.txt')
            with open(txt_file, 'w', encoding='utf-8') as f:
                f.write(chunk['text'])

        return {
            'success': True,
            'chunks': len(chunks),
            'duration': metadata['total_duration'],
            'avg_chunk_duration': avg_duration
        }

    except Exception as e:
        error_msg = str(e)
        if "429" in error_msg or "Too Many Requests" in error_msg:
            return {
                'success': False,
                'error': 'RateLimited',
                'chunks': 0
            }
        return {
            'success': False,
            'error': error_msg[:100],
            'chunks': 0
        }

# ==============================
# MAIN LOOP
# ==============================

print("🚀 Starting downloads...")
print("=" * 70)
print()

processed = 0
rate_limited = False
results = []
all_chunk_durations = []

for i, video_id in enumerate(batch_videos):
    current_index = START_INDEX + i
    total_done = current_index + 1

    print(f"📥 [{total_done}/{len(all_video_ids)}] Downloading: {video_id}")

    result = download_transcript_chunks(video_id)

    if result['success']:
        avg_dur = result.get('avg_chunk_duration', 0)
        print(f"   ✅ Success: {result['chunks']} chunks, avg {avg_dur:.1f}s each")
        all_chunk_durations.append(avg_dur)
        downloaded.add(video_id)
    else:
        if result['error'] == 'RateLimited':
            print(f"   🛑 Rate limited - stopping")
            rate_limited = True
            break
        else:
            print(f"   ❌ Error: {result['error']}")

    results.append({
        'video_id': video_id,
        'success': result['success'],
        'chunks': result.get('chunks', 0),
        'avg_chunk_duration': result.get('avg_chunk_duration', 0),
        'error': result.get('error', '')
    })

    processed += 1

    # Save progress
    if processed % SAVE_EVERY == 0:
        with open(progress_file, 'w') as f:
            json.dump({
                'completed': list(downloaded),
                'total': len(all_video_ids),
                'timestamp': time.time()
            }, f)
        print(f"   💾 Progress saved ({total_done} total)")

    # Delay
    if i < len(batch_videos) - 1:
        wait_time = random.uniform(MIN_WAIT, MAX_WAIT)
        time.sleep(wait_time)

# Final progress save
with open(progress_file, 'w') as f:
    json.dump({
        'completed': list(downloaded),
        'total': len(all_video_ids),
        'timestamp': time.time()
    }, f)

# ==============================
# SUMMARY
# ==============================

print("\n" + "=" * 70)
print("✅ BATCH COMPLETE")
print("=" * 70)
print()

success_count = sum(1 for r in results if r['success'])
total_chunks = sum(r['chunks'] for r in results)

print(f"📊 This batch: {processed} videos")
print(f"✅ Successful: {success_count}/{processed}")
print(f"📝 Total chunks: {total_chunks}")
print()

# Chunk duration statistics
if all_chunk_durations:
    avg_chunk = sum(all_chunk_durations) / len(all_chunk_durations)
    min_chunk = min(all_chunk_durations)
    max_chunk = max(all_chunk_durations)
    print(f"⏱️  Chunk duration statistics:")
    print(f"   Average: {avg_chunk:.1f}s")
    print(f"   Min avg: {min_chunk:.1f}s")
    print(f"   Max avg: {max_chunk:.1f}s")
    print(f"   Target range: {MIN_CHUNK_DURATION}-{MAX_CHUNK_DURATION}s ✅")
    print()

print(f"💾 Saved to: {OUTPUT_DIR}/")
print(f"📈 Overall progress: {len(downloaded)}/{len(all_video_ids)} videos")

if rate_limited:
    print()
    print("⚠️  RATE LIMITED - Wait 1-2 hours and re-run with RESUME=True")

print("=" * 70)

# Save batch summary
if results:
    summary_df = pd.DataFrame(results)
    summary_file = os.path.join(OUTPUT_DIR, f'batch_summary_{START_INDEX}.csv')
    summary_df.to_csv(summary_file, index=False)
    print(f"\n💾 Batch summary: {summary_file}")

!zip -rq bangla_transcripts_16.zip /content/bangla_transcripts_16 > /dev/null 2>&1
from google.colab import files
files.download('bangla_transcripts_16.zip')
