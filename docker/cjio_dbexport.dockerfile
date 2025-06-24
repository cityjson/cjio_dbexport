FROM python:3.12.11-bullseye
ARG VERSION
LABEL org.opencontainers.image.authors="Balázs Dukai <balazs.dukai@3dgi.nl>"
LABEL org.opencontainers.image.vendor="3DGI"
LABEL org.opencontainers.image.title="cjio_dbexport"
LABEL org.opencontainers.image.description="Export tool from PostGIS to CityJSON."
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.licenses="MIT"

WORKDIR /usr/src/cjio_dbexport

COPY AUTHORS.rst CHANGELOG.rst LICENSE README.rst pyproject.toml ./
COPY cjio_dbexport ./cjio_dbexport
RUN pip install --no-cache-dir .

CMD [ "cjdb" ]