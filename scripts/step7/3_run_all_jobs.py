#!/usr/bin/env python3
"""
Upload PySpark jobs to S3 and run all jobs in parallel on EMR Serverless.

Usage:
    python3 3_run_all_jobs.py [OPTIONS]
    python3 scripts/step7/3_run_all_jobs.py [OPTIONS]

Options:
    --skip-upload        Skip uploading jobs to S3 (assumes already uploaded)
    -w, --wait           Wait for all jobs to complete (default: submit and exit)
    --sequential         Run jobs sequentially instead of parallel (not recommended)
    -h, --help           Show this help message

Examples:
    # From scripts/step7/ directory:
    python3 3_run_all_jobs.py                    # Upload and submit all jobs in parallel
    python3 3_run_all_jobs.py --wait           # Upload, submit all jobs, wait for all to complete
    python3 3_run_all_jobs.py --skip-upload --wait  # Skip upload, submit all jobs, wait for completion
    
    # From project root:
    python3 scripts/step7/3_run_all_jobs.py --wait
    python3 scripts/step7/3_run_all_jobs.py --skip-upload --wait
"""
import sys
import os
import argparse
import subprocess
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


def submit_job(job_name, script_dir, wait=False):
    """Submit a single job and return job run ID."""
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    
    wait_flag = ["--wait"] if wait else []
    
    try:
        result = subprocess.run(
            [sys.executable, "-u", str(script_dir / "2_run_job.py"), job_name] + wait_flag,
            check=True,
            env=env,
            capture_output=True,
            text=True
        )
        # Extract job run ID from output if available
        output = result.stdout
        job_run_id = None
        for line in output.split('\n'):
            if 'Job Run ID:' in line:
                job_run_id = line.split('Job Run ID:')[-1].strip()
                break
        return {"job": job_name, "status": "submitted", "job_run_id": job_run_id, "output": output}
    except subprocess.CalledProcessError as e:
        return {"job": job_name, "status": "failed", "error": e.stderr, "output": e.stdout}
    except Exception as e:
        return {"job": job_name, "status": "error", "error": str(e)}


def main():
    parser = argparse.ArgumentParser(
        description="Upload PySpark jobs to S3 and run all jobs in parallel on EMR Serverless."
    )
    parser.add_argument("--skip-upload", action="store_true", help="Skip uploading jobs to S3 (assumes already uploaded)")
    parser.add_argument("-w", "--wait", action="store_true", help="Wait for all jobs to complete after submission")
    parser.add_argument("--sequential", action="store_true", help="Run jobs sequentially (not recommended, use parallel instead)")
    
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    
    # Step 1: Upload jobs to S3 (unless skipped)
    if not args.skip_upload:
        print("=" * 60)
        print("Step 1: Uploading jobs to S3")
        print("=" * 60)
        print()
        sys.stdout.flush()
        
        try:
            # Set PYTHONUNBUFFERED for child process
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            
            result = subprocess.run(
                [sys.executable, "-u", str(script_dir / "1_upload_jobs.py")],
                check=True,
                env=env
            )
        except subprocess.CalledProcessError as e:
            print("✗ Failed to upload jobs", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"✗ Error running upload script: {e}", file=sys.stderr)
            sys.exit(1)
        
        print()
        sys.stdout.flush()
    else:
        print("Skipping upload (--skip-upload specified)")
        print()
        sys.stdout.flush()
    
    # Step 2: Run all jobs (parallel or sequential)
    print("=" * 60)
    if args.sequential:
        print("Step 2: Running all jobs sequentially on EMR Serverless")
    else:
        print("Step 2: Submitting all jobs in parallel to EMR Serverless")
    print("=" * 60)
    print()
    sys.stdout.flush()
    
    # Job execution order with dependencies:
    # 1. create_lookup_tables.py - Independent, creates reference tables
    # 2. data_validation_cleaning.py - MUST run first, creates processed/trips_cleaned/
    # 3-5. Insight jobs - Can run in parallel AFTER step 2 completes (all read from processed/trips_cleaned/)
    jobs_phase1 = [
        "create_lookup_tables.py",  # Independent
    ]
    
    jobs_phase2 = [
        "data_validation_cleaning.py",  # MUST complete before phase 3
    ]
    
    jobs_phase3 = [
        "trip_metrics_aggregation.py",  # Depends on processed/trips_cleaned/
        "geospatial_analysis.py",       # Depends on processed/trips_cleaned/
        "revenue_insights.py",          # Depends on processed/trips_cleaned/
    ]
    
    # For backward compatibility, if sequential mode, use old flat list
    if args.sequential:
        jobs = jobs_phase1 + jobs_phase2 + jobs_phase3
    else:
        jobs = []  # Will be handled in phases
    
    total_jobs = len(jobs_phase1) + len(jobs_phase2) + len(jobs_phase3)
    
    if args.sequential:
        # Sequential execution (old behavior)
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        wait_flag = ["--wait"] if args.wait else []
        
        for idx, job in enumerate(jobs, 1):
            print("=" * 60)
            print(f"Job {idx}/{total_jobs}: {job}")
            print("=" * 60)
            print()
            sys.stdout.flush()
            
            try:
                result = subprocess.run(
                    [sys.executable, "-u", str(script_dir / "2_run_job.py"), job] + wait_flag,
                    check=True,
                    env=env
                )
            except subprocess.CalledProcessError as e:
                print()
                print(f"✗ Job {idx}/{total_jobs} ({job}) failed", file=sys.stderr)
                sys.stderr.flush()
                sys.exit(1)
            except Exception as e:
                print(f"✗ Error running job {job}: {e}", file=sys.stderr)
                sys.stderr.flush()
                sys.exit(1)
            
            print()
            print(f"✓ Job {idx}/{total_jobs} ({job}) completed successfully")
            print()
            sys.stdout.flush()
    else:
        # Phased parallel execution respecting dependencies
        print("=" * 60)
        print("Phase 1: Independent jobs (lookup tables)")
        print("=" * 60)
        print()
        sys.stdout.flush()
        
        # Phase 1: Run independent jobs (lookup tables)
        phase1_results = []
        if jobs_phase1:
            for job in jobs_phase1:
                result = submit_job(job, script_dir, wait=args.wait)
                phase1_results.append(result)
                if result["status"] == "submitted":
                    print(f"✓ {job} - Submitted successfully")
                    if result.get("job_run_id"):
                        print(f"  Job Run ID: {result['job_run_id']}")
                else:
                    print(f"✗ {job} - Failed to submit")
                    if result.get("error"):
                        print(f"  Error: {result['error']}")
                sys.stdout.flush()
        
        print()
        print("=" * 60)
        print("Phase 2: Data cleaning (MUST complete before insights)")
        print("=" * 60)
        print()
        sys.stdout.flush()
        
        # Phase 2: Run data cleaning and wait for completion
        phase2_results = []
        if jobs_phase2:
            for job in jobs_phase2:
                print(f"Submitting {job} (will wait for completion)...")
                result = submit_job(job, script_dir, wait=True)  # Always wait for cleaning job
                phase2_results.append(result)
                if result["status"] == "submitted":
                    print(f"✓ {job} - Completed successfully")
                else:
                    print(f"✗ {job} - Failed")
                    if result.get("error"):
                        print(f"  Error: {result['error']}")
                    print("\n⚠ Data cleaning failed. Insight jobs depend on cleaned data.")
                    print("Stopping execution. Please fix the data cleaning job before proceeding.")
                    sys.exit(1)
                sys.stdout.flush()
        
        print()
        print("=" * 60)
        print("Phase 3: Insight jobs (can run in parallel)")
        print("=" * 60)
        print()
        sys.stdout.flush()
        
        # Phase 3: Run insight jobs in parallel
        phase3_results = []
        if jobs_phase3:
            print(f"Submitting {len(jobs_phase3)} insight jobs in parallel...")
            print()
            sys.stdout.flush()
            
            with ThreadPoolExecutor(max_workers=len(jobs_phase3)) as executor:
                future_to_job = {
                    executor.submit(submit_job, job, script_dir, wait=args.wait): job 
                    for job in jobs_phase3
                }
                
                for idx, future in enumerate(as_completed(future_to_job), 1):
                    job_name = future_to_job[future]
                    try:
                        result = future.result()
                        phase3_results.append(result)
                        
                        if result["status"] == "submitted":
                            print(f"✓ [{idx}/{len(jobs_phase3)}] {job_name} - Submitted successfully")
                            if result.get("job_run_id"):
                                print(f"  Job Run ID: {result['job_run_id']}")
                        else:
                            print(f"✗ [{idx}/{len(jobs_phase3)}] {job_name} - Failed to submit")
                            if result.get("error"):
                                print(f"  Error: {result['error']}")
                        sys.stdout.flush()
                    except Exception as e:
                        print(f"✗ [{idx}/{len(jobs_phase3)}] {job_name} - Error: {e}", file=sys.stderr)
                        sys.stderr.flush()
                        phase3_results.append({"job": job_name, "status": "error", "error": str(e)})
        
        # Summary
        print()
        print("=" * 60)
        print("Job Execution Summary")
        print("=" * 60)
        
        all_results = phase1_results + phase2_results + phase3_results
        successful = [r for r in all_results if r["status"] == "submitted"]
        failed = [r for r in all_results if r["status"] != "submitted"]
        
        print(f"✓ Successfully completed: {len(successful)}/{len(all_results)}")
        if failed:
            print(f"✗ Failed: {len(failed)}/{len(all_results)}")
            for r in failed:
                print(f"  - {r['job']}: {r.get('error', 'Unknown error')}")
        
        print()
        print("Monitor job status with:")
        print("  aws emr-serverless list-job-runs --application-id <app-id>")
        print()
        sys.stdout.flush()
    
    sys.stdout.flush()


if __name__ == "__main__":
    main()
