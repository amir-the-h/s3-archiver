# S3 Archiver

Make an archive from a directory and all subsequent subdirectories and objects in S3.
The trick is will upload the archive directly to S3, without having to download it to the local machine.

## Installation
- Clone the repository to your local machine:
```bash
git clone https://github.com/amir-the-h/s3-archiver.git
```
- Install the dependencies:
```bash
cd s3-archiver
npm install
```

## Usage
```bash
npm run start s3-archiver <bucket> <directory>
```

## Result
The archive will be uploaded to the bucket root.