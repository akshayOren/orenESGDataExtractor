# Running the Malaysia Scraper on AWS — Beginner's Guide

This guide explains how to run `scripts/download_malaysia_reports.py` at full
scale (~100,000 PDFs, ~300 GB) using Amazon Web Services (AWS). It's written
assuming you've never used AWS before.

Read it top to bottom the first time. Skim it the second time. After that you
won't need it.

---

## Table of Contents

1. [The Problem We're Solving](#1-the-problem-were-solving)
2. [AWS Jargon Decoded](#2-aws-jargon-decoded)
3. [Two Ways to Do This — Pick One](#3-two-ways-to-do-this--pick-one)
4. [Approach A — Laptop + S3 (Simplest)](#4-approach-a--laptop--s3-simplest)
5. [Approach B — EC2 + S3 (More Reliable)](#5-approach-b--ec2--s3-more-reliable)
6. [How Much Will This Cost?](#6-how-much-will-this-cost)
7. [Cleanup Checklist — Do Not Skip](#7-cleanup-checklist--do-not-skip)
8. [Common Beginner Mistakes](#8-common-beginner-mistakes)
9. [Glossary](#9-glossary)

---

## 1. The Problem We're Solving

The Malaysia scraper downloads ~100,000 PDF files (about 300 GB total) from
Bursa Malaysia's website. Two practical issues:

- **Disk space** — most laptops don't have 300 GB free.
- **Time** — at ~2 files/second, the whole run takes ~12–24 hours. If your
  laptop sleeps, loses Wi-Fi, or you close the lid, the job stops.

**AWS solves both**: you rent storage (cheap, unlimited) and optionally a
computer that never sleeps (also cheap).

---

## 2. AWS Jargon Decoded

AWS has hundreds of services with strange names. You only need five.

| AWS name | What it really is | Real-world analogy |
|---|---|---|
| **S3** | Cloud file storage | A rented warehouse for your files |
| **EC2** | A rented computer in Amazon's data center | Renting a desktop PC by the hour |
| **IAM** | "Who is allowed to do what" | Keycards and door permissions at an office |
| **Region** | Which city the data center is in | `us-east-1` = Virginia, `ap-southeast-1` = Singapore |
| **Bucket** | One folder in S3 | One shelf in the warehouse |

Two terms you'll see but don't need to deeply understand:

| Term | What it means |
|---|---|
| **EBS** | The hard drive attached to your rented EC2 computer |
| **Security Group** | Firewall rules: "who can connect to this computer" |

---

## 3. Two Ways to Do This — Pick One

### Approach A: Laptop + S3
You run the scraper **on your own laptop**. The PDFs land on your laptop disk
first. When done, you upload them all to S3 and delete the local copy.

- **Good if**: You have ~300 GB free on your laptop and stable home internet.
- **Simpler**: Fewer moving parts.
- **Risk**: If your laptop reboots or Wi-Fi drops mid-run, you have to restart.
  (The script resumes, so this is annoying but not fatal.)

### Approach B: EC2 + S3
You rent a small computer on AWS (**EC2**), run the scraper on that computer,
and it writes directly to S3. Your laptop can sleep; the scraper keeps running.

- **Good if**: Your internet/laptop isn't reliable, or you want to "fire and
  forget" for 24 hours.
- **More steps**, but more robust.
- **~$1 extra** for the rental computer.

**Recommendation for a first-timer**: start with **Approach A**. If it works,
you've learned S3. If your laptop can't finish, graduate to Approach B.

---

## 4. Approach A — Laptop + S3 (Simplest)

### Step 1 — Create an AWS account

Skip if you already have one. Go to <https://aws.amazon.com> → "Create an AWS
Account". You'll need a credit card, but nothing we do here will cost more than
a few dollars.

### Step 2 — Create an IAM user for programmatic access

Never use your AWS root login from the command line. Create a separate user
with limited permissions.

1. AWS Console → search "IAM" → Users → **Create user**
2. User name: `oren-esg-scraper`
3. Attach policies directly → check **AmazonS3FullAccess**
   (We'll tighten this later. Full access is fine while learning.)
4. Create user.
5. Click the new user → **Security credentials** tab → **Create access key**
6. Pick **"Command Line Interface (CLI)"**
7. You'll see an **Access key ID** and **Secret access key**. Copy both now —
   the secret is never shown again.

**Treat these keys like a password.** Anyone with them can spend money on your
account.

### Step 3 — Install the AWS CLI on your laptop

The AWS CLI is a program that talks to AWS from your terminal.

**Windows** (you're on Windows based on your setup):
```bash
# Download and install from:
# https://awscli.amazonaws.com/AWSCLIV2.msi

# Verify after install:
aws --version
# should print: aws-cli/2.x.x ...
```

### Step 4 — Configure the AWS CLI with your keys

```bash
aws configure
```

It will ask four questions:
- **AWS Access Key ID**: paste the key from Step 2
- **AWS Secret Access Key**: paste the secret from Step 2
- **Default region name**: `us-east-1` (Virginia — cheapest) or
  `ap-southeast-1` (Singapore — faster to reach Bursa)
- **Default output format**: `json`

Test it:
```bash
aws sts get-caller-identity
# should print your account ID and user ARN
```

### Step 5 — Create an S3 bucket

A bucket is your cloud folder. Bucket names must be **globally unique** across
all of AWS, so pick something like `oren-esg-malaysia-<your-initials>`.

```bash
aws s3 mb s3://oren-esg-malaysia-yourname --region us-east-1
```

If it complains "bucket already exists", someone picked that name — pick
another.

Verify:
```bash
aws s3 ls
# should show your new bucket
```

### Step 6 — Run the scraper on your laptop

You already know this command:
```bash
venv/Scripts/python.exe scripts/download_malaysia_reports.py --from 2024-01-01
```

Let it run. Come back in 12–24 hours. PDFs are landing in
`data/reports/malaysia/`.

**Tip**: disable Windows sleep while it runs (Settings → System → Power → Screen
and sleep → "Never" for both). Resume is supported if it stops, but avoid
unnecessary restarts.

### Step 7 — Upload to S3 when done

```bash
aws s3 sync data/reports/malaysia s3://oren-esg-malaysia-yourname/reports/malaysia
```

What `sync` does: compares your local folder against the S3 bucket and uploads
only the files that aren't there yet. Safe to re-run — it won't re-upload.

Verify the upload:
```bash
aws s3 ls s3://oren-esg-malaysia-yourname/reports/malaysia/ --recursive --human-readable --summarize
# should show all files and total size near the bottom
```

### Step 8 — (Optional) Delete local copy

Once you've confirmed everything is in S3, you can reclaim laptop disk:
```bash
# Be very sure first. This is permanent.
rm -rf data/reports/malaysia
```

**Done.** Skip to [Section 6](#6-how-much-will-this-cost) for cost info and
[Section 7](#7-cleanup-checklist--do-not-skip) for cleanup.

---

## 5. Approach B — EC2 + S3 (More Reliable)

You rent a small Linux computer, run the scraper on it, and let it write to S3.

Do Steps 1–5 from Approach A first (account, IAM user, AWS CLI, S3 bucket).

### Step 1 — Create an IAM role for EC2 → S3

This is **different from a user**. A role lets an EC2 computer use AWS
services without hardcoded keys.

1. IAM → Roles → **Create role**
2. Trusted entity type: **AWS service**
3. Use case: **EC2**
4. Permissions: attach **AmazonS3FullAccess**
5. Role name: `oren-esg-ec2-role`
6. Create.

### Step 2 — Launch an EC2 instance

1. Console → EC2 → **Launch instance**
2. Name: `oren-malaysia-scraper`
3. AMI: **Ubuntu Server 22.04 LTS** (free tier eligible)
4. Instance type: **t3.small** (2 vCPU, 2 GB RAM — plenty for this IO-bound job)
5. Key pair: **Create new key pair** → name it `oren-key` → RSA → .pem → Download.
   **Save the .pem file somewhere safe — you cannot re-download it.**
6. Network settings → **Edit**:
   - Auto-assign public IP: **Enable**
   - Security group: create new
     - SSH from **My IP** (not "Anywhere" — that's insecure)
     - Do NOT add any other rules
7. Storage: 30 GB gp3 (the default 8 GB is too small for logs)
8. **Advanced details** (scroll down) → IAM instance profile: pick
   `oren-esg-ec2-role` from Step 1.
9. **Launch instance**

Wait ~30 seconds for it to boot. Note its **Public IPv4 address** (e.g.
`54.123.45.67`).

### Step 3 — Connect to the instance

From Git Bash on Windows:
```bash
# Lock down the key file (SSH refuses it otherwise)
chmod 400 ~/Downloads/oren-key.pem

# Connect
ssh -i ~/Downloads/oren-key.pem ubuntu@54.123.45.67
# Type "yes" to trust the fingerprint. You're now on the remote computer.
```

### Step 4 — Set up the environment on EC2

```bash
# Update packages
sudo apt update && sudo apt install -y python3-pip python3-venv tmux git

# Clone the repo (you'll need to push yours to GitHub first,
# or use git bundle/scp — ask if stuck)
git clone https://github.com/<your-org>/orenESGDataExtractor.git
cd orenESGDataExtractor

# Create venv and install
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Verify AWS credentials come from the IAM role (no aws configure needed)
aws sts get-caller-identity
# should show the role ARN ending in "oren-esg-ec2-role/..."
```

### Step 5 — Run in tmux (so it survives your disconnect)

`tmux` is a program that keeps your session alive even when you disconnect.

```bash
tmux new -s scrape

# Inside tmux:
source venv/bin/activate
python scripts/download_malaysia_reports.py --from 2024-01-01 2>&1 | tee logs/run.log

# To detach (leave it running): press Ctrl-B, then D
# You can now safely close your laptop. The job keeps running.

# To reconnect later:
ssh -i ~/Downloads/oren-key.pem ubuntu@54.123.45.67
tmux attach -t scrape
```

### Step 6 — Upload to S3

PDFs are currently on the EC2 disk. Sync them to S3:

```bash
aws s3 sync data/reports/malaysia s3://oren-esg-malaysia-yourname/reports/malaysia
```

Because EC2 and S3 are in the same region, this transfer is **free and fast**.

### Step 7 — Shut down the EC2 instance

**This is critical. Forgetting to shut down is the #1 source of AWS surprise
bills.**

```bash
# From your laptop (not the EC2):
aws ec2 terminate-instances --instance-ids i-0abc123...  # get ID from console
```

Or in the console: EC2 → Instances → select your instance → Instance state →
**Terminate (delete)**.

"Stop" pauses but still charges for storage. "Terminate" deletes the instance
entirely. For a one-off job, always terminate.

---

## 6. How Much Will This Cost?

### One-time scrape cost

| Item | What you're paying for | Cost |
|---|---|---|
| EC2 t3.small on-demand, 24 hrs | The rental computer | ~$0.50 |
| EC2 t3.small **spot**, 24 hrs (alternative) | Discounted computer | ~$0.15 |
| EBS 30 GB for 1 day | Hard drive on the computer | ~$0.08 |
| S3 PUT requests, ~100k | Writing files to cloud storage | ~$0.50 |
| Data IN from Bursa → AWS | Downloading PDFs into AWS | **FREE** |
| Data between EC2 and S3 (same region) | Internal AWS transfer | **FREE** |
| **One-time total** | | **~$1–2** |

### Ongoing monthly storage cost (for the 300 GB)

| Storage class | $/GB/month | 300 GB / month | Use when |
|---|---|---|---|
| S3 Standard | $0.023 | **$6.90** | You access files often (daily/weekly) |
| S3 Standard-IA | $0.0125 | $3.75 | Access ≤ 1× per month |
| S3 Glacier Instant Retrieval | $0.004 | $1.20 | Archive, rare access |
| S3 Glacier Deep Archive | $0.00099 | $0.30 | Cold archive, OK waiting 12 hrs |

**Recommendation**: keep it in Standard while the pipeline is actively parsing
these PDFs. After a month, set a **lifecycle rule** to transition to Glacier
Instant Retrieval. Cost drops from $7/month to $1.20/month automatically.

### The two costs to watch out for

1. **Forgetting to terminate EC2** → silently charges ~$15/month for a t3.small
   sitting idle.
2. **Downloading data OUT of AWS** → $0.09/GB. Pulling all 300 GB back to your
   laptop later = $27. **Run the rest of the pipeline inside AWS** to avoid this.

### Set a budget alert

Do this once, then forget it:

1. AWS Console → search "Budgets" → **Create budget**
2. Budget type: **Cost budget**
3. Amount: $10/month
4. Alert email: your email
5. Alert at 80% of actual ($8)

If anything ever goes wrong and AWS starts charging you unexpectedly, you'll
get an email before it becomes a big deal.

---

## 7. Cleanup Checklist — Do Not Skip

After the scrape is done, go through this list. Each unchecked item could cost
you money for months.

- [ ] **Terminate** (not just stop) any EC2 instances you launched
- [ ] Delete any **Elastic IPs** you allocated (they charge while unattached)
- [ ] **Keep** the S3 bucket — that's your data — but set a lifecycle rule if
      you don't need it in hot storage
- [ ] Confirm in the **Billing dashboard** that nothing unexpected is charging
- [ ] Confirm your **IAM access keys** aren't committed to git anywhere
- [ ] Rotate the IAM access keys if you suspect they leaked

How to quickly check for orphaned resources:
```bash
# Running EC2 instances (any region)
aws ec2 describe-instances --query "Reservations[].Instances[?State.Name=='running'].InstanceId"

# Unattached Elastic IPs
aws ec2 describe-addresses --query "Addresses[?!AssociationId].PublicIp"

# S3 buckets (you should see your one bucket)
aws s3 ls
```

---

## 8. Common Beginner Mistakes

| Mistake | What happens | How to avoid |
|---|---|---|
| Not terminating EC2 after the job | ~$15/month charge forever | Terminate in console the moment the job finishes |
| Hardcoding AWS access keys in code | Keys leak to GitHub → crypto-miners use your account | Use `aws configure` or IAM roles, never put keys in files |
| Using "Anywhere" for SSH security group | Bots hammer your SSH port | Set source to "My IP" |
| Launching in an expensive region | 20–30% higher bills | Prefer `us-east-1` |
| Using a NAT Gateway | $14+ in data processing fees for 300 GB | Put EC2 in a **public** subnet with a public IP |
| "Stopping" instead of "Terminating" | EBS storage keeps charging | Always terminate for one-off jobs |
| Downloading all 300 GB back to laptop | $27 egress charge | Run downstream pipeline in AWS too |
| Making the S3 bucket public by accident | Private company data becomes public | Keep "Block all public access" ON |

---

## 9. Glossary

- **AMI (Amazon Machine Image)**: a snapshot of an operating system you can
  launch an EC2 instance from. Ubuntu, Amazon Linux, Windows — all are AMIs.
- **ARN (Amazon Resource Name)**: a unique identifier for any AWS thing. Looks
  like `arn:aws:s3:::my-bucket`.
- **Availability Zone**: a specific data center within a region. A region has
  multiple AZs for resilience. Irrelevant for our job.
- **Bucket**: a top-level "folder" in S3. Names are globally unique.
- **EBS (Elastic Block Store)**: the hard disk attached to an EC2 instance.
- **EC2 (Elastic Compute Cloud)**: AWS's rented-computer service.
- **Egress**: data leaving AWS. It costs money.
- **IAM (Identity and Access Management)**: the permissions system.
- **IAM Role**: permissions granted to an AWS service (like EC2) to use other
  services (like S3), without access keys.
- **IAM User**: a human account, with access keys for API/CLI use.
- **Instance**: a single running EC2 computer.
- **Lifecycle Rule**: an automatic rule that moves S3 objects between storage
  classes (e.g. Standard → Glacier after 30 days).
- **On-Demand**: pay-per-hour EC2 pricing, no commitment.
- **Region**: a geographic AWS location, e.g. `us-east-1` (Virginia).
- **S3 (Simple Storage Service)**: AWS's cloud file storage.
- **Security Group**: firewall rules around EC2 instances.
- **Spot Instance**: a heavily discounted EC2 that AWS can reclaim with 2
  minutes' notice. Good for resumable jobs like ours.
- **SSH Key Pair**: a public/private key used to log into a Linux EC2 instance.
- **Terminate**: permanently delete an EC2 instance. Opposite of "launch".
- **VPC (Virtual Private Cloud)**: a virtual network in AWS. You get a default
  one; don't touch it.

---

## If You Get Stuck

Common symptoms and fixes:

| Symptom | Likely cause | Fix |
|---|---|---|
| `aws: command not found` | CLI not installed | Re-do Approach A, Step 3 |
| `AccessDenied` writing to S3 | IAM user/role lacks S3 permission | Attach `AmazonS3FullAccess` |
| SSH "Permission denied (publickey)" | Wrong `.pem` file or wrong user | `ubuntu@` for Ubuntu AMIs, `ec2-user@` for Amazon Linux |
| Script gets `403` from Bursa | Rate-limited or impersonation issue | Increase `--sleep` to 1.0, retry in an hour |
| S3 upload looks stuck | Slow home internet | Normal for 300 GB; check speed with `aws s3 sync --debug` if worried |
| EC2 instance won't connect | Security group blocks your IP | Edit security group → SSH from "My IP" (your IP changes on Wi-Fi reconnects) |

If nothing works, screenshot the error and ask me in the next session.
