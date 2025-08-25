# Dockerfile for a Go dev environment with Oh My Zsh

FROM golang:1.22

RUN apt-get update && apt-get install -y zsh git wget \
    && rm -rf /var/lib/apt/lists/*

# 安装 Oh My Zsh (使用非交互式方法)
RUN sh -c "$(wget -O- https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended

# 下载 zsh-autosuggestions 和 zsh-syntax-highlighting 插件
RUN git clone https://github.com/zsh-users/zsh-autosuggestions ${ZSH_CUSTOM:-/root/.oh-my-zsh/custom}/plugins/zsh-autosuggestions
RUN git clone https://github.com/zsh-users/zsh-syntax-highlighting.git ${ZSH_CUSTOM:-/root/.oh-my-zsh/custom}/plugins/zsh-syntax-highlighting

# 修改 .zshrc 配置文件，启用插件
RUN sed -i 's/plugins=(git)/plugins=(git zsh-autosuggestions zsh-syntax-highlighting)/' /root/.zshrc

CMD ["zsh"]
