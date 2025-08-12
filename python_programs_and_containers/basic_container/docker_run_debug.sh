docker run -it --rm --network host  \
  --mount type=bind,source=/home/gedgar/mount_startup,target=/mount_startup \
  --mount type=bind,source=/home/gedgar/mount_secrets,target=/mount_secrets \
  --mount type=bind,source=/home/gedgar/site_configuration,target=/app/site_configuration \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e system_create=true \
  -e secrets_load=true \
  nanodatacenter/python_base_image /bin/bash
