import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    _conv_b64,
    _is_valid_uuid,
    lro,
)
from typing import Optional, Tuple
import sempy_labs._icons as icons
from sempy.fabric.exceptions import FabricHTTPException
from uuid import UUID
import pandas as pd


def list_dataflows(workspace: Optional[str] = None):
    """
    Shows a list of all dataflows which exist within a workspace.

    Parameters
    ----------
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        A pandas dataframe showing the dataflows which exist within a workspace.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    client = fabric.PowerBIRestClient()
    response = client.get(f"/v1.0/myorg/groups/{workspace_id}/dataflows")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    df = pd.DataFrame(
        columns=["Dataflow Id", "Dataflow Name", "Configured By", "Users", "Generation"]
    )

    for v in response.json().get("value", []):
        new_data = {
            "Dataflow Id": v.get("objectId"),
            "Dataflow Name": v.get("name"),
            "Configured By": v.get("configuredBy"),
            "Users": [v.get("users")],
            "Generation": v.get("generation"),
        }
        df = pd.concat(
            [df, pd.DataFrame(new_data, index=[0])],
            ignore_index=True,
        )

    df["Generation"] = df["Generation"].astype(int)

    return df


def _resolve_dataflow_name_and_id(
    dataflow: str | UUID, workspace: Optional[str] = None
) -> Tuple[str, UUID]:

    if workspace is None:
        workspace = fabric.resolve_workspace_name(workspace)

    dfD = list_dataflows(workspace=workspace)

    if _is_valid_uuid(dataflow):
        dfD_filt = dfD[dfD["Dataflow Id"] == dataflow]
    else:
        dfD_filt = dfD[dfD["Dataflow Name"] == dataflow]

    if len(dfD_filt) == 0:
        raise ValueError(
            f"{icons.red_dot} The '{dataflow}' dataflow does not exist within the '{workspace}' workspace."
        )

    dataflow_id = dfD_filt["Dataflow Id"].iloc[0]
    dataflow_name = dfD_filt["Dataflow Name"].iloc[0]

    return dataflow_name, dataflow_id


def create_dataflow(
    dataflow_name: str,
    mashup_document: str,
    description: Optional[str] = None,
    workspace: Optional[str] = None,
):
    """
    Creates a Dataflow Gen2 based on a mashup document.

    Parameters
    ----------
    dataflow_name : str
        Name of the dataflow.
    mashup_docment : str
        The mashup document (use the '' function).
    description : str, default=None
        The description of the dataflow.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)

    payload = {
        "displayName": dataflow_name,
        "type": "Dataflow",
    }

    if description is not None:
        payload["description"] = description

    dataflow_def_payload = {
        "editingSessionMashup": {
            "mashupName": "",
            "mashupDocument": mashup_document,
            "queryGroups": [],
            "documentLocale": "en-US",
            "gatewayObjectId": None,
            "queriesMetadata": None,
            "connectionOverrides": [],
            "trustedConnections": None,
            "useHostConnectionProvider": False,
            "fastCombine": False,
            "allowNativeQueries": True,
            "allowedModules": None,
            "skipAutomaticTypeAndHeaderDetection": False,
            "disableAutoAnonymousConnectionUpsert": None,
            "hostProperties": {
                "DataflowRefreshOutpuFileFormat": "Parquet",
                "EnableDataTimeFieldsForStaging": True,
                "EnablePublishWithoutLoadedQueries": True,
            },
            "defaultOutputDestinationConfiguration": None,
            "stagingDefinition": None,
        }
    }

    dataflow_def_payload_conv = _conv_b64(dataflow_def_payload)

    payload["definition"] = {
        "parts": [
            {
                "path": "dataflow-content.json",
                "payload": dataflow_def_payload_conv,
                "payloadType": "InlineBase64",
            }
        ]
    }

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspace_id}/items", json=payload)

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{dataflow_name}' has been created within the '{workspace}' workspace."
    )


def delete_dataflow(dataflow: str, workspace: Optional[str] = None):

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = fabric.resolve_item_id(
        item_name=dataflow, type="Dataflow", workspace=workspace_id
    )

    client = fabric.FabricRestClient()
    response = client.delete(f"/v1/workspaces/{workspace_id}/items/{item_id}")

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{dataflow}' dataflow within the '{workspace_name}' workspace has been deleted."
    )


def run_dataflow_job(dataflow: str, workspace: Optional[str] = None):

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = fabric.resolve_item_id(
        item_name=dataflow, type="Dataflow", workspace=workspace_id
    )
    client = fabric.FabricRestClient()

    payload = {
        "executionData": {
            "PipelineName": "Dataflow",
            "OwnerUserPrincipalName": "<name@email.com>",
            "OwnerUserObjectId": "<ObjectId>",
        }
    }

    response = client.post(
        f"/v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances?jobType=Refresh",
        json=payload,
    )

    if response.status_code not in [200, 202]:
        raise FabricHTTPException(response)

    print(
        f"{icons.in_progress} The '{dataflow}' dataflow within the '{workspace_name}' workspace was initiated."
    )

    result = lro(client, response).json()
    job_instance_id = result["jobInstanceId"]

    # Check the progress
    job_status = "InProgress"
    while job_status not in ["Completed", "Failed"]:
        response = client.get(
            f"/v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances/{job_instance_id}"
        )
        if response.status_code != 200:
            raise FabricHTTPException(response)
        job_status = response.json().get("status")

    if job_status == "Completed":
        print(
            f"{icons.green_dot} The '{dataflow}' dataflow within the '{workspace_name}' workspace '{job_status.lower()}."
        )
    else:
        print(
            f"{icons.red_dot} The '{dataflow}' dataflow within the '{workspace_name}' workspace '{job_status.lower()}."
        )


def cancel_dataflow_job(
    dataflow: str, job_instance_id: UUID, workspace: Optional[str] = None
):

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    item_id = fabric.resolve_item_id(
        item_name=dataflow, type="Dataflow", workspace=workspace_id
    )
    client = fabric.FabricRestClient()

    response = client.post(
        f"/v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances/{job_instance_id}/cancel"
    )

    if response.status_code != 200:
        raise FabricHTTPException(response)

    print(
        f"{icons.green_dot} The '{dataflow}' dataflow within the '{workspace_name}' workspace's job has been cancelled."
    )
