FROM mcr.microsoft.com/dotnet/core/sdk:3.1

RUN apt update \
	&& ln -fs /usr/share/zoneinfo/Europe/London /etc/localtime \
	&& DEBIAN_FRONTEND=noninteractive apt install -y tzdata \
	&& DEBIAN_FRONTEND=noninteractive dpkg-reconfigure --frontend noninteractive tzdata \
	&& rm -rf /var/lib/apt/lists/*

RUN apt update \
	&& apt install -y python3 \
	&& rm -rf /var/lib/apt/lists/*

ENV PYTHON=python3

