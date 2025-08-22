FROM httpd:2.4

# enable proxy modules
RUN sed -i '/^#LoadModule proxy_module/s/^#//' /usr/local/apache2/conf/httpd.conf \
 && sed -i '/^#LoadModule proxy_http_module/s/^#//' /usr/local/apache2/conf/httpd.conf

# include our proxy rules (use printf, not Bash $'...')
COPY am-proxy.conf /usr/local/apache2/conf/extra/am-proxy.conf
RUN printf '\nInclude conf/extra/am-proxy.conf\n' >> /usr/local/apache2/conf/httpd.conf

# site
COPY ./public/ /usr/local/apache2/htdocs/