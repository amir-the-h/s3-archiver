const { S3 } = require('aws-sdk');
const { PassThrough } = require('stream');
const Archiver = require('archiver');

// get inputs from the user
const bucket = process.argv[2];
const directory = process.argv[3];
const zipFileName = process.argv[4];

// check the inputs
if (!bucket || !directory || !zipFileName) {
  console.log('Useaage: npm run start <bucket> <directory> <zipFileName>');
  process.exit(1);
}

const uploadParams = {
  Bucket: bucket,
  ContentType: 'application/zip',
  Key: directory + zipFileName,
}
let filesList = [];
const uploadedFileParts = [];
const failedFileParts = [];
const streamPassThrough = new PassThrough();
const s3 = new S3();

// minUploadSize is the minimum size of a file to be uploaded in bytes
const minUploadSize = 10485760; // 10 MB

let totalFiles = 0;
let archivedFiles = 0;
let upload = null;

const archive = Archiver('zip', {});

async function main() {
  archive.pipe(streamPassThrough);
  archive
    .on('error', (err) => {
      console.error(`ZIP ERROR: ${err}`);
    })
    .on('progress', (progress) => {
      archivedFiles = progress.entries.processed;
      console.log(`Archived ${archivedFiles} of ${totalFiles} files`);
      if (archivedFiles === totalFiles) {
        console.log('All files processed');
        archive.finalize();
      }
    });

  console.log('Creating upload request');
  upload = await createUpload();
  console.log(`Upload created with ID: ${upload.UploadId}`);

  console.log(`Getting files from ${bucket}/${directory}`);
  await getFilesList();
  console.log(`Found ${filesList.length} files`);

  // Create a new download stream for the files
  for (const fileKey of filesList) {
    processFileObject(fileKey);
  };

  console.log('Starting upload');
  await startUpload();
  console.log('Upload done');

  console.log('Retrying failed parts');
  await retryFailedParts();
  console.log('Retry done');

  return completeUpload();
}

async function getFilesList(marker) {
  // Call S3 to list current buckets
  const params = {
    Bucket: bucket,
    Prefix: directory,
    ContinuationToken: marker,
  };

  // List objects in the S3 bucket
  let err, data = await s3.listObjectsV2(params).promise();
  totalFiles += data.Contents.length;

  if (err) {
    // Failed to get list of objects
    console.error('Error getting object list', err);
    return;
  } else {
    filesList = filesList.concat(data.Contents.map(file => file.Key));

    if (data.IsTruncated)
      await getFilesList(data.NextContinuationToken);
  }
}

async function processFileObject(fileKey) {
  // Wait for the part to be uploaded
  const object = await s3.getObject({ Bucket: bucket, Key: fileKey }).promise();
  archive.append(object.Body, { name: fileKey });
}

function createUpload() {
  return new Promise((resolve, reject) => {
    s3.createMultipartUpload(uploadParams, (err, data) => {
      if (err) return reject(err);
      return resolve(data);
      /*
        data = {
        Key: "<Your Key for the file>",
        Bucket: "<Your Bucket_name>",
        UploadId: "ibZBv_75gd9r8lH_gqXatLdxMVpAlj6ZQjEs.Sjng--" //some jebrish!
        }
      */
    });
  })
}

async function startUpload() {
  return new Promise(async (resolve, reject) => {
    // Make a buffer and fill it with the stream
    let buffer = Buffer.from([]);
    let partNumber = 1;
    streamPassThrough
      .on('data', (chunk) => {
        // add the chunk to the buffer
        buffer = Buffer.concat([buffer, chunk]);
        // if the buffer is greater than the minimum upload size
        while (buffer.byteLength >= minUploadSize) {
          // get the minimum upload size buffer
          const minUploadBuffer = buffer.slice(0, minUploadSize);
          // update the buffer
          buffer = buffer.slice(minUploadSize);
          // upload the buffer
          uploadPart(minUploadBuffer, partNumber)
            .then((part) => {
              uploadedFileParts.push(part);
            })
            .catch((failedPart) => {
              failedFileParts.push(failedPart);
            });
          partNumber++;
        }
      });

    // wait for files to be added
    while (archivedFiles < totalFiles) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    if (buffer.byteLength > 0) {
      console.log('All files processed');
      uploadPart(buffer, partNumber)
        .then((part) => {
          uploadedFileParts.push(part);
          resolve();
        })
        .catch((failedPart) => {
          failedFileParts.push(failedPart);
          resolve();
        });
    } else {
      resolve();
    }
  });
}

async function retryFailedParts() {
  while (failedFileParts.length > 0) {
    for (const failedPart of failedFileParts) {
      if (failedPart.retry !== true) {
        failedPart.retry = true;
        uploadPart(failedPart.buffer, failedPart.PartNumber)
          .then((part) => {
            uploadedFileParts.push(part);
            failedFileParts.splice(failedFileParts.indexOf(failedPart), 1);
          })
          .catch((failedPart) => {
            // remove the old part from the list
            failedFileParts.splice(failedFileParts.indexOf(failedPart), 1);
            // add the new part to the list
            failedFileParts.push(failedPart);
          });
      }
    }
  }
}

function uploadPart(buffer, partNumber) {
  return new Promise((resolve, reject) => {
    const partParams = Object.assign({}, uploadParams, {
      Body: buffer,
      UploadId: upload.UploadId,
      PartNumber: partNumber,
    });

    // Delete ContentType from params
    delete partParams.ContentType;

    console.log(`Uploading part ${partNumber}`);
    s3.uploadPart(partParams).promise()
      .then((data) => {
        console.log(`Uploaded part ${partNumber}`);
        resolve({ PartNumber: partNumber, ETag: data.ETag });
      })
      .catch((err) => {
        console.error(`Error uploading part ${partNumber}`, err);
        reject({ PartNumber: partNumber, error: err, buffer: buffer, retry: false });
      });
  });
}

async function completeUpload() {
  // Sort the uploaded parts in ascending order by part number
  uploadedFileParts.sort((a, b) => {
    return a.PartNumber - b.PartNumber;
  });
  const params = Object.assign({}, uploadParams, {
    UploadId: upload.UploadId,
    MultipartUpload: {
      Parts: uploadedFileParts,
    },
  });

  // Delete ContentType from params
  delete params.ContentType;

  return s3.completeMultipartUpload(params).promise();
}

main()
  .then(() => {
    console.log('Done');
  })
  .catch(err => {
    console.error(err);
  });