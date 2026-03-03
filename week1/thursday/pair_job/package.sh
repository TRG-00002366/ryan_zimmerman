#!/bin/bash
# Package the application for submission

# Create a zip of Python modules
zip -r app.zip sales_processor.py utils.py

echo "Package created: app.zip"