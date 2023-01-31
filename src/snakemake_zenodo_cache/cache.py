import hashlib
import inspect
from pathlib import Path
from typing import List, Union
from snakemake.io import directory


class ZenodoCache:
    def __init__(self, cache_directory: Union[str, Path]):
        frame = inspect.currentframe().f_back
        self.workflow = frame.f_globals["workflow"]
        self.checkpoints = frame.f_globals["checkpoints"]
        if isinstance(cache_directory, Path):
            cache_directory = str(cache_directory)
        cache_directory = self.workflow.modifier.path_modifier.modify(cache_directory)
        self.cache_directory = Path(cache_directory)
        self._create_download_rule()

    def _add_rule(self, name, **kwargs):
        rule_name = self.workflow.add_rule(name=name, **kwargs)
        rule = self.workflow.get_rule(rule_name)
        rule.resources = {"_cores": 1}
        rule.input_modifier = self.workflow.modifier.path_modifier
        rule.output_modifier = self.workflow.modifier.path_modifier
        return rule

    def _create_download_rule(self) -> None:
        rule = self._add_rule("_zenodo_cache_download")
        rule.set_output(directory(self.cache_directory / "download"))
        rule.run_func = self._download
        rule.is_checkpoint = True
        self.workflow.globals["checkpoints"].register(rule)
        self._download_rule = rule

    def _download(self, input: List[str], output: List[str], *_) -> List[Path]:
        del input
        Path(output[0]).mkdir(parents=True, exist_ok=True)

    def to_cache(self, filename: Union[str, Path]) -> Union[str, Path]:
        h = hashlib.sha1()
        if isinstance(filename, Path):
            h.update(str(filename).encode())
        else:
            h.update(filename.encode())
        file_id = h.hexdigest()

        rule = self._add_rule(f"_zenodo_cache_{file_id}")

        def input_from_rule_or_cache(*_):
            ckp = self.workflow.globals["checkpoints"]._zenodo_cache_download.get()
            print(type(ckp.output[0]))
            return []

        rule.set_input(input_from_rule_or_cache)
        rule.set_output(self.from_cache(filename))

        rule.run_func = lambda *args: print(f"{filename}:", args)

        return filename

    def from_cache(self, filename: Union[str, Path]) -> Path:
        return self.cache_directory / filename
