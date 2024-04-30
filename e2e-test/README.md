To run an e2e test on your own bee, outside of GHA:
```commandline
cd e2e-test
export BEE_NAME={your-bee-name}
export BILLING_PROJECT_NAME={your-bee-billing-project}
#for wds
export AZURE_TOKEN={bearer-token-for-a-user-on-your-bee}
#for cromwell
export BEARER_TOKEN={bearer-token-for-a-user-on-your-bee}
python {cbas|cromwell|wds}_azure_e2etest.py
```
Don't forget to delete test-created workspaces from your BEE when you are done if needed!