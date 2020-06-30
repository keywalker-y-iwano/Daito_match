FROM python:3
USER root
#ラベル名

RUN apt-get update
RUN apt-get -y install locales && \
    localedef -f UTF-8 -i ja_JP ja_JP.UTF-8
ENV LANG ja_JP.UTF-8
ENV LANGUAGE ja_JP:ja
ENV LC_ALL ja_JP.UTF-8
ENV TZ JST-9
ENV TERM xterm
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/id_rsa

RUN apt-get install -y vim less
RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install --upgrade numpy
RUN pip install --upgrade pandas
RUN pip install --upgrade paramiko
RUN pip install --upgrade mojimoji

ADD https://raw.githubusercontent.com/keywalker-y-iwano/Daito_match/master/kw-Daito_matching.py ./
ADD https://raw.githubusercontent.com/keywalker-y-iwano/Daito_match/master/input/id_rsa ./