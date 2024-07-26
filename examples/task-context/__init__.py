from engine import register, retries, PipelineExecutor, Task
import requests


@register()
class NewTenant(Task):
    @retries(3)
    def perform_task(self):

        print(self.context)

        response = requests.post(self.context['tenant_info']['config_api_url'])

        if response.status_code == 400:
            raise Exception("Tenant already exists 400")
        print("Creating new tenant")

    def retry_handler(self, error):
        if "400" in str(error):
            return False
        return True


if __name__ == "__main__":
    executor = PipelineExecutor()
    executor.run()
