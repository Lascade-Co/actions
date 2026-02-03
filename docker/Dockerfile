FROM eclipse-temurin:21-jdk-jammy

ARG ANDROID_SDK_ROOT=/opt/android-sdk
ARG CMDLINE_TOOLS_VERSION=11076708
ARG ANDROID_PLATFORM=35
ARG BUILD_TOOLS_VERSION=35.0.0
ARG NDK_VERSION=26.3.11579264

ENV DEBIAN_FRONTEND=noninteractive \
    ANDROID_HOME=${ANDROID_SDK_ROOT} \
    ANDROID_SDK_ROOT=${ANDROID_SDK_ROOT} \
    PATH=${ANDROID_SDK_ROOT}/cmdline-tools/latest/bin:${ANDROID_SDK_ROOT}/platform-tools:${PATH}

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    curl \
    git \
    gnupg \
    openssh-client \
    openssl \
    python3 \
    unzip \
    xz-utils \
    zip \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get update \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p ${ANDROID_SDK_ROOT}/cmdline-tools \
    && curl -fsSL "https://dl.google.com/android/repository/commandlinetools-linux-${CMDLINE_TOOLS_VERSION}_latest.zip" -o /tmp/cmdline-tools.zip \
    && unzip -q /tmp/cmdline-tools.zip -d ${ANDROID_SDK_ROOT}/cmdline-tools \
    && mv ${ANDROID_SDK_ROOT}/cmdline-tools/cmdline-tools ${ANDROID_SDK_ROOT}/cmdline-tools/latest \
    && rm -f /tmp/cmdline-tools.zip

RUN yes | sdkmanager --sdk_root=${ANDROID_SDK_ROOT} --licenses \
    && sdkmanager --sdk_root=${ANDROID_SDK_ROOT} \
      "platform-tools" \
      "platforms;android-${ANDROID_PLATFORM}" \
      "build-tools;${BUILD_TOOLS_VERSION}" \
      "ndk;${NDK_VERSION}" \
    && rm -rf ${ANDROID_SDK_ROOT}/.android \
    && rm -rf /root/.android
