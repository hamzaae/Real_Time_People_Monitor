FROM ubuntu:20.04

# Set the working directory
WORKDIR /app

# Install essential tools and sudo
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    wget \
    git \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Install Python
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Set up a non-root user with sudo privileges
RUN useradd -ms /bin/bash dockeruser && \
    usermod -aG sudo dockeruser

# Set password for the user (replace 'password' with your desired password)
RUN echo 'dockeruser:password' | chpasswd

USER dockeruser

# Set the default command to run when the container starts
CMD ["/bin/bash"]
