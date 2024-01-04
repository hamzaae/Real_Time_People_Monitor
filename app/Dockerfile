FROM ubuntu

# Set the working directory
WORKDIR /rtdbpc

COPY . .

# Install essential tools and sudo
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    wget \
    nano \
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

RUN pip3 install -r requirements.txt
RUN pip3 install -r yolov5/requirements.txt
RUN pip3 install 'git+https://github.com/facebookresearch/detectron2.git'

# Set password for the user (replace 'password' with your desired password)
RUN echo 'dockeruser:password' | chpasswd

USER dockeruser

# Set the default command to run when the container starts
CMD ["/bin/bash"]
