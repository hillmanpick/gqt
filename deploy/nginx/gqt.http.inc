location = /gqt {
    return 301 https://$host/gqt/;
}

location ^~ /gqt/ {
    return 301 https://$host$request_uri;
}
