from jinja2 import Environment, FileSystemLoader
from pathlib import Path
import subprocess
import json

from dotenv import load_dotenv
import re
from prefect.blocks.system import JSON
from prefect.filesystems import GitHub


class GeneratePrefectAirbyteJinjaTemplate():
    _template_name: str= 'prefect_airbyte_jinja_template.txt'
    _prefect_agent_airbyte_github_name = "github-prefect-airbyte-connections-orchestration"
    _prefect_agent_airbyte_github = GitHub.load(_prefect_agent_airbyte_github_name)
    github_repo_name =  re.search(r'[a-zA-z0-9]+',re.search(r'\/[a-zA-z0-9]+.git',_prefect_agent_airbyte_github.repository).group()).group()
    github_branch = _prefect_agent_airbyte_github.reference

    def __init__(self, airbyte_object_name):
        self.airbyte_object_name = airbyte_object_name
        self._write_file_location = self._set_write_file_location()
        self.file_name = self._create_file_name()
        self.file_location = Path.joinpath(self._write_file_location, self.file_name)
        self.flow_name = self._create_flow_name()
        self.slack_channel, self.list_connection = self._get_slack_channel_and_list_connection() 

    def _create_file_name(self):
        return self.airbyte_object_name.replace('-','_') + '.py'
    
    def _create_flow_name(self):
        return self.airbyte_object_name.replace('-','_') + '_main_flow'
    
    def _set_write_file_location(self):
        if self.github_repo_name not in str(Path.cwd().parent):
            _write_file_location = Path.cwd() / self.github_repo_name
        else:
            _write_file_location = Path.cwd().parent
        return _write_file_location
    
    def _get_slack_channel_and_list_connection(self):
        for i in JSON.load(self.airbyte_object_name).value:
            for k,v in i.items():
                if k == 'slack_channel':
                    slack_channel = v
                elif k == 'list_connection':
                    list_connection = v
        return slack_channel, list_connection

    def generate_prefect_airbyte_jinja_template(self):
        template_location = ['jinja_template', Path.joinpath(self._write_file_location,'code_generator/jinja_template')]
        r: object = Environment(
            loader=FileSystemLoader(template_location), 
            extensions=['jinja2.ext.do']
        )
        load_template = r.get_template(self._template_name)

        with open(Path.joinpath(self._write_file_location, self.file_name), 'w') as f:
            f.write(load_template.render(
                prefect_agent_airbyte_github_name=self._prefect_agent_airbyte_github_name,
                slack_channel = self.slack_channel,
                list_app_and_connection = self.list_connection, 
                flow_name = self.flow_name,
                prefect_deployement_name=self.file_name.replace('.py','')
            ))
    
    def push_generated_template_to_prefect_airbyte_github(self):
        command = f'git add --all && git commit -m "update {self.file_name}" && git push origin {self.github_branch}'
        run = subprocess.run(command, shell=True, cwd=self._write_file_location, capture_output=True)
        return run

    def create_prefect_deployment(self):
        command = f'python {self.file_name} --deploy "true"'
        run = subprocess.run(command, shell=True, cwd=self._write_file_location, capture_output=True, check=True)
        return run
    
# for local testing and avoid importing
if __name__ == "__main__":
    pass
    # airbyte_object = GeneratePrefectAirbyteJinjaTemplate('airbyte-connection-pion')
    # airbyte_object.generate_prefect_airbyte_jinja_template()