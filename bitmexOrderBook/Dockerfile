FROM python:latest


# Set the working directory to /app
WORKDIR /bitmexOrderBook

# Copy the current directory contents into the container at /app
ADD . /bitmexOrderBook

COPY requirements.txt requirements.txt

# Install any needed packages specified in requirements.txt
RUN pip install -r  requirements.txt

COPY . .

ENTRYPOINT python app.py

