# Use an official Python runtime as the base image
FROM python:3.9.18-slim

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy the local package files to the container's workspace
COPY . .

RUN pip install -e .

# Define the default command to run when starting the container
CMD [ "bash" ]