#!/usr/bin/env python3
"""
SMART DOWNLOADER WITH AUTO GITHUB SYNC
Automatically pulls existing transcripts from GitHub before starting
Automatically pushes new transcripts after downloading
"""

# !pip -q install youtube-transcript-api pandas
import pandas as pd
import time
import random
import os
import json
import subprocess
from youtube_transcript_api import YouTubeTranscriptApi
import youtube_transcript_api._errors as yt_errors

# ==============================
# CONFIG - EDIT THESE
# ==============================
INPUT_CSV = "videos_with_bangla.csv"
OUTPUT_DIR = "bangla_transcripts"
VIDEO_ID_COLUMN = "video_id"

# GitHub settings
GITHUB_REPO = "https://github.com/Sanjidx090/ytranscript.git"
GITHUB_TOKEN = ""  # Optional: for private repos
ENABLE_AUTO_SYNC = True  # Set to False to disable GitHub sync

# Chunking
MIN_CHUNK_DURATION = 20
MAX_CHUNK_DURATION = 30

# Batch
START_INDEX = 0
BATCH_SIZE = 50

# Safety
MIN_WAIT = 2.0
MAX_WAIT = 4.0
SAVE_EVERY = 5

# ==============================
# GITHUB SYNC FUNCTIONS
# ==============================

def run_git_command(cmd):
    """Run git command silently"""
    try:
        subprocess.run(cmd, shell=True, capture_output=True, check=True)
        return True
    except:
        return False

def pull_from_github():
    """Pull existing transcripts from GitHub"""
    if not ENABLE_AUTO_SYNC:
        return
    
    print("=" * 70)
    print("📥 PULLING FROM GITHUB")
    print("=" * 70)
    print()
    
    # Configure git
    run_git_command('git config --global user.name "Auto Sync"')
    run_git_command('git config --global user.email "sync@auto.com"')
    
    # Check if already a repo
    if os.path.exists('.git'):
        print("📦 Pulling latest changes...")
        if run_git_command('git pull origin main'):
            print("   ✅ Pulled successfully")
        else:
            print("   ⚠️  Pull failed (continuing anyway)")
    else:
        print("🆕 Cloning repository...")
        repo_url = GITHUB_REPO
        if GITHUB_TOKEN:
            repo_url = GITHUB_REPO.replace('https://', f'https://{GITHUB_TOKEN}@')
        
        if run_git_command(f'git clone {repo_url} .'):
            print("   ✅ Cloned successfully")
        else:
            print("   ⚠️  Clone failed (starting fresh)")
            run_git_command('git init')
            run_git_command(f'git remote add origin {repo_url}')
    
    print()

def push_to_github(video_count):
    """Push new transcripts to GitHub"""
    if not ENABLE_AUTO_SYNC:
        return
    
    print()
    print("=" * 70)
    print("📤 PUSHING TO GITHUB")
    print("=" * 70)
    print()
    
    print("📦 Adding files...")
    run_git_command('git add .')
    
    print("💾 Committing...")
    run_git_command(f'git commit -m "Auto-sync: {video_count} videos"')
    
    print("📤 Pushing...")
    if run_git_command('git push origin main'):
        print("   ✅ Pushed to GitHub!")
    else:
        # Try creating branch if it doesn't exist
        if run_git_command('git push --set-upstream origin main'):
            print("   ✅ Pushed to GitHub!")
        else:
            print("   ⚠️  Push failed (check token/permissions)")
    
    print()

# ==============================
# SETUP
# ==============================

# Pull from GitHub first
if ENABLE_AUTO_SYNC:
    pull_from_github()

os.makedirs(OUTPUT_DIR, exist_ok=True)
progress_file = os.path.join(OUTPUT_DIR, "download_progress.json")

print("=" * 70)
print("SMART DOWNLOADER WITH AUTO GITHUB SYNC")
print("=" * 70)
print(f"Chunk size: {MIN_CHUNK_DURATION}-{MAX_CHUNK_DURATION}s (random, respects words)")
print(f"Output: {OUTPUT_DIR}/")
if ENABLE_AUTO_SYNC:
    print(f"GitHub: {GITHUB_REPO}")
print()

# ==============================
# LOAD VIDEO LIST
# ==============================
df = pd.read_csv(INPUT_CSV)
all_video_ids = df[VIDEO_ID_COLUMN].dropna().astype(str).unique().tolist()
print(f"📌 Total videos in CSV: {len(all_video_ids)}")

# Check what's already downloaded (from GitHub or previous runs)
downloaded = set()
if os.path.exists(progress_file):
    with open(progress_file, 'r') as f:
        progress = json.load(f)
        downloaded = set(progress.get('completed', []))

# Also check actual folders
existing_folders = set()
if os.path.exists(OUTPUT_DIR):
    for item in os.listdir(OUTPUT_DIR):
        item_path = os.path.join(OUTPUT_DIR, item)
        if os.path.isdir(item_path) and item != '.git':
            existing_folders.add(item)

# Combine both
downloaded = downloaded.union(existing_folders)

if downloaded:
    print(f"✅ Already downloaded: {len(downloaded)} videos (from GitHub or local)")

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
# CHUNKING FUNCTION
# ==============================
def chunk_smart_duration(segments, min_duration=20, max_duration=30):
    """Create smart chunks respecting word boundaries"""
    if not segments:
        return []
    
    chunks = []
    chunk_id = 0
    i = 0
    
    while i < len(segments):
        target_duration = random.uniform(min_duration, max_duration)
        chunk_start = segments[i].start
        chunk_segments = []
        chunk_texts = []
        current_duration = 0
        
        while i < len(segments):
            seg = segments[i]
            seg_duration = seg.duration
            
            if not chunk_segments:
                chunk_segments.append(seg)
                chunk_texts.append(seg.text)
                current_duration += seg_duration
                i += 1
                continue
            
            if current_duration < min_duration:
                chunk_segments.append(seg)
                chunk_texts.append(seg.text)
                current_duration += seg_duration
                i += 1
                continue
            
            new_duration = current_duration + seg_duration
            if current_duration >= target_duration or new_duration > max_duration:
                break
            
            chunk_segments.append(seg)
            chunk_texts.append(seg.text)
            current_duration += seg_duration
            i += 1
        
        if chunk_segments:
            chunk_end = chunk_segments[-1].start + chunk_segments[-1].duration
            chunks.append({
                'chunk_id': chunk_id,
                'start': chunk_start,
                'end': chunk_end,
                'duration': chunk_end - chunk_start,
                'text': " ".join(chunk_texts),
                'segments': len(chunk_segments)
            })
            chunk_id += 1
    
    return chunks

# ==============================
# DOWNLOAD FUNCTION
# ==============================
def download_transcript_chunks(video_id):
    """Download and chunk transcript"""
    try:
        transcript = api.fetch(video_id, languages=['bn'])
        segments = list(transcript)
        
        if not segments:
            return {'success': False, 'error': 'No segments', 'chunks': 0}
        
        chunks = chunk_smart_duration(segments, MIN_CHUNK_DURATION, MAX_CHUNK_DURATION)
        
        video_dir = os.path.join(OUTPUT_DIR, video_id)
        os.makedirs(video_dir, exist_ok=True)
        
        chunk_durations = [c['duration'] for c in chunks]
        avg_duration = sum(chunk_durations) / len(chunk_durations) if chunk_durations else 0
        
        metadata = {
            'video_id': video_id,
            'url': f'https://www.youtube.com/watch?v={video_id}',
            'total_chunks': len(chunks),
            'avg_chunk_duration': avg_duration,
            'total_duration': segments[-1].start + segments[-1].duration
        }
        
        with open(os.path.join(video_dir, 'metadata.json'), 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        for chunk in chunks:
            chunk_file = os.path.join(video_dir, f'chunk_{chunk["chunk_id"]:04d}.json')
            with open(chunk_file, 'w', encoding='utf-8') as f:
                json.dump(chunk, f, ensure_ascii=False, indent=2)
            
            txt_file = os.path.join(video_dir, f'chunk_{chunk["chunk_id"]:04d}.txt')
            with open(txt_file, 'w', encoding='utf-8') as f:
                f.write(chunk['text'])
        
        return {'success': True, 'chunks': len(chunks), 'avg_chunk_duration': avg_duration}
        
    except Exception as e:
        error_msg = str(e)
        if "429" in error_msg or "Too Many Requests" in error_msg:
            return {'success': False, 'error': 'RateLimited', 'chunks': 0}
        return {'success': False, 'error': error_msg[:100], 'chunks': 0}

# ==============================
# MAIN LOOP
# ==============================
print("🚀 Starting downloads...")
print("=" * 70)
print()

processed = 0
rate_limited = False

for i, video_id in enumerate(batch_videos):
    total_done = len(downloaded) + processed + 1
    
    print(f"📥 [{total_done}/{len(all_video_ids)}] Downloading: {video_id}")
    
    result = download_transcript_chunks(video_id)
    
    if result['success']:
        avg_dur = result.get('avg_chunk_duration', 0)
        print(f"   ✅ Success: {result['chunks']} chunks, avg {avg_dur:.1f}s each")
        downloaded.add(video_id)
    else:
        if result['error'] == 'RateLimited':
            print(f"   🛑 Rate limited - stopping")
            rate_limited = True
            break
        else:
            print(f"   ❌ Error: {result['error']}")
    
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
    
    if i < len(batch_videos) - 1:
        time.sleep(random.uniform(MIN_WAIT, MAX_WAIT))

# Final save
with open(progress_file, 'w') as f:
    json.dump({
        'completed': list(downloaded),
        'total': len(all_video_ids),
        'timestamp': time.time()
    }, f)

# ==============================
# PUSH TO GITHUB
# ==============================
if ENABLE_AUTO_SYNC:
    push_to_github(len(downloaded))

# ==============================
# SUMMARY
# ==============================
print("=" * 70)
print("✅ BATCH COMPLETE")
print("=" * 70)
print()
print(f"📊 This batch: {processed} videos")
print(f"📈 Total downloaded: {len(downloaded)}/{len(all_video_ids)} videos")
print()

if ENABLE_AUTO_SYNC:
    print(f"🔗 GitHub: {GITHUB_REPO}")
    print(f"📦 {len(downloaded)} videos synced to GitHub")
    print()

if rate_limited:
    print("⚠️  RATE LIMITED")
    print("   Next steps:")
    print("   1. Wait 1-2 hours OR switch to different platform")
    print("   2. Run this script again (it will pull from GitHub and continue)")

print("=" * 70)
