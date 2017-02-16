FROM mysql:5.7

ENV COMPOSER_ALLOW_SUPERUSER 1

RUN apt-get update && apt-get install -y --no-install-recommends \
		ca-certificates \
		curl \
		git \
		php5-cli \
		php5-mysql \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /code/
RUN curl --fail -sS https://getcomposer.org/installer | php \
	&& mv composer.phar /usr/local/bin/composer

ENV MYSQL_ROOT_PASSWORD root

COPY . /code/

RUN /usr/local/bin/composer install \
	&& chmod uga+rwx /code/entrypoint.sh \
	&& printf "[mysqld]\nsecure-file-priv=\"\"\n" >> /etc/mysql/my.cnf

ENTRYPOINT /entrypoint.sh mysqld & php /code/console.php hierarchy:run --data=/data/
