const UPLOAD_API_URL =
  "https://1ptrorx4i9.execute-api.ap-southeast-2.amazonaws.com/ingestions/upload-url";

const form = document.querySelector("#upload-form");
const fileInput = document.querySelector("#csv-file");
const uploadButton = document.querySelector("#upload-button");
const statusBox = document.querySelector("#status");

function showStatus(message, type) {
  statusBox.hidden = false;
  statusBox.className = `status ${type}`;
  statusBox.textContent = message;
}

async function readJson(response) {
  const text = await response.text();
  try {
    return text ? JSON.parse(text) : {};
  } catch {
    throw new Error(`API returned an unexpected response (HTTP ${response.status})`);
  }
}

async function requestUploadUrl() {
  const response = await fetch(UPLOAD_API_URL, {
    method: "POST",
    headers: {
      Accept: "application/json",
    },
  });

  const body = await readJson(response);
  if (!response.ok) {
    throw new Error(body.message || `Could not create upload URL (HTTP ${response.status})`);
  }

  if (!body.upload_url || !body.job_id) {
    throw new Error("The API response did not include an upload URL and job ID");
  }

  return body;
}

async function uploadToS3(uploadUrl, file) {
  const response = await fetch(uploadUrl, {
    method: "PUT",
    headers: {
      "Content-Type": "text/csv",
    },
    body: file,
  });

  if (!response.ok) {
    throw new Error(`S3 upload failed (HTTP ${response.status})`);
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();

  const file = fileInput.files[0];

  if (!file) {
    showStatus("Choose a CSV file first.", "error");
    return;
  }

  if (!file.name.toLowerCase().endsWith(".csv")) {
    showStatus("Please choose a file ending in .csv.", "error");
    return;
  }

  uploadButton.disabled = true;
  showStatus("Creating upload URL...", "info");

  try {
    const upload = await requestUploadUrl();
    showStatus(`Uploading ${file.name}...`, "info");
    await uploadToS3(upload.upload_url, file);
    showStatus(
      `Upload complete.\nJob ID: ${upload.job_id}\n\nS3 will now process the file in the background.`,
      "success",
    );
    form.reset();
  } catch (error) {
    showStatus(error instanceof Error ? error.message : "Upload failed.", "error");
  } finally {
    uploadButton.disabled = false;
  }
});
