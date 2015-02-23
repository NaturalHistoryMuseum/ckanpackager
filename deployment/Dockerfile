FROM ubuntu:12.04

#
# Install Python, virtualenv, supervisor
#
RUN apt-get update && apt-get install -y python python-virtualenv git build-essential python-dev libxml2-dev libxslt1-dev zip supervisor wget

#
# Install Redis
#
RUN cd /root                                                      \
    && wget http://download.redis.io/releases/redis-2.8.19.tar.gz \
    && tar -xzf redis-2.8.19.tar.gz                               \
    && cd redis-2.8.19                                            \
    && make                                                       \
    && make install 

#
# Install ckanpackager & dependencies in a virtualenv.
#
RUN virtualenv /usr/lib/ckanpackager                                                    \
    && cd /usr/lib/ckanpackager                                                         \
    && . /usr/lib/ckanpackager/bin/activate                                             \
    && pip install -e git+https://github.com/NaturalHistoryMuseum/ckanpackager.git#egg=ckanpackager  \
    && pip install -r /usr/lib/ckanpackager/src/ckanpackager/requirements.txt           \
    && mkdir /etc/ckanpackager                                                          \
    && mkdir /var/lib/ckanpackager                                                      \
    && mkdir -p /var/www/ckanpackager

COPY ckanpackager_settings.py /etc/ckanpackager/ckanpackager_settings.py
COPY gbif_dwca_extensions /etc/ckanpackager/gbif_dwca_extensions
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY celery_flower.sh /usr/local/bin/celery_flower.sh
COPY redis.conf /etc/redis.conf

ENV CKANPACKAGER_CONFIG=/etc/ckanpackager/ckanpackager_settings.py

EXPOSE 8765
EXPOSE 5555
CMD ["/usr/bin/supervisord"]
