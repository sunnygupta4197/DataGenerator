---
name: Data Upload Request
about: Upload a JSON configuration file for DataGenerator processing
title: 'Process Data: [Brief Description]'
labels: ['data-upload']
assignees: ''

---

## 📋 Data Processing Request

Please provide the following information to process your data file:

### Required Information
**File URL:** 
<!-- Paste the direct link to your JSON configuration file here -->
<!-- Examples: 
- GitHub Gist raw URL: https://gist.githubusercontent.com/username/gistid/raw/filename.json
- Direct file URL: https://example.com/config.json
- Temporary file sharing service URL
-->

### Optional Parameters
**Rows:** 10
<!-- Number of rows to generate (default: 10) -->

**Workers:** 4
<!-- Number of workers to use (default: 4) -->

**Memory:** 1024
<!-- Memory limit in MB (default: 1024) -->

### Additional Information
<!-- 
Provide any additional context about your data processing needs:
- Description of the data you're generating
- Any special requirements or considerations
- Expected output format details
-->

---

## 📝 Instructions for File Upload

Since GitHub doesn't allow direct file uploads in issues, you can use one of these methods:

### Option 1: GitHub Gist (Recommended)
1. Go to https://gist.github.com/
2. Create a new gist with your JSON file
3. Copy the "Raw" URL and paste it above

### Option 2: Temporary File Sharing
1. Upload your file to a service like:
   - https://file.io/
   - https://tmpfiles.org/
   - https://0x0.st/
2. Copy the direct download link and paste it above

### Option 3: Your Own Hosting
1. Upload to your own web server, cloud storage, or file hosting
2. Ensure the URL provides direct access to the JSON file
3. Paste the URL above

---

## ⚠️ Important Notes

- **File Format**: Only JSON files are supported
- **File Size**: Keep files under 10MB for best performance  
- **URL Access**: Ensure the URL is publicly accessible (no authentication required)
- **Processing Time**: Larger datasets may take several minutes to process
- **Results**: You'll receive a comment on this issue with download links when processing is complete

---

## 🔧 Troubleshooting

If you encounter issues:
- Ensure your JSON file is properly formatted
- Verify the URL is accessible in a browser
- Check that the `data-upload` label is applied to this issue
- Wait a few moments for the workflow to trigger automatically

---

*This issue will be automatically processed by the DataGenerator workflow. Please do not remove the `data-upload` label.*