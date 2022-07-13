FROM pybuild:latest as pybuild

USER ${user}

COPY --chown=${user} pyproject.toml poetry.lock ./
RUN poetry install --no-dev --no-root

COPY --chown=${user} aioconveyor/ aioconveyor/
COPY --chown=${user} config/ config/
COPY --chown=${user} paii/ paii/
COPY --chown=${user} utils/ utils/

RUN poetry build
RUN pip install dist/*whl

FROM pyrun:latest as pyrun

COPY --chown=${user} --from=pybuild ${venv} ${venv}

CMD ["bash"]
