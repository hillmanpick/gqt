location = /gqt {
    return 301 /gqt/;
}

location ^~ /gqt/ {
    proxy_pass http://172.18.0.1:4173/;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-Prefix /gqt;
    proxy_connect_timeout 5s;
    proxy_read_timeout 130s;
    client_max_body_size 2m;
}
