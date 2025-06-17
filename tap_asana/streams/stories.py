import singer
from tap_asana.context import Context
from tap_asana.streams.base import Stream

LOGGER = singer.get_logger()

class Stories(Stream):
    name = "stories"
    replication_method = "INCREMENTAL"
    replication_key = "created_at"

    fields = [
        "gid",
        "resource_type",
        "created_at",
        "created_by",
        "resource_subtype",
        "text",
        "html_text",
        "is_pinned",
        "assignee",
        "dependency",
        "duplicate_of",
        "duplicated_from",
        "follower",
        "hearted",
        "hearts",
        "is_edited",
        "liked",
        "likes",
        "new_approval_status",
        "new_dates",
        "new_enum_value",
        "old_date_value",
        "new_date_value",
        "old_people_value",
        "new_people_value",
        "new_name",
        "new_number_value",
        "new_resource_subtype",
        "new_section",
        "new_text_value",
        "num_hearts",
        "num_likes",
        "old_approval_status",
        "old_dates",
        "old_enum_value",
        "old_name",
        "old_number_value",
        "old_resource_subtype",
        "old_section",
        "old_text_value",
        "preview",
        "project",
        "source",
        "story",
        "tag",
        "target",
        "task",
        "sticker_name",
        "custom_field",
        "is_editable",
        "new_multi_enum_values",
        "old_multi_enum_values",
        "type"
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_gids = self.get_project_gids()

    def get_objects(self):
        """Get stream object"""
        bookmark = self.get_bookmark()
        session_bookmark = bookmark
        modified_since = bookmark.strftime("%Y-%m-%dT%H:%M:%S.%f")
        opt_fields = ",".join(self.fields)

        # iterate over all project ids and continue fetching
        LOGGER.info("Fetching stories...")
        projects_total = len(self.project_gids)
        projects_fraction = max(projects_total // 100, 1)

        for indx, project_gid in enumerate(self.project_gids, 1):
            if (indx % projects_fraction == 0):
                LOGGER.info(f"Fetching done for projects: {indx - 1}/{projects_total}")

            for task in self.call_api(
                    "tasks",
                    project=project_gid,
                    modified_since=modified_since,
            ):
                task_gid = task.get("gid")
                for story in Context.asana.client.stories.get_stories_for_task(
                        task_gid=task_gid,
                        opt_fields=opt_fields,
                        timeout=self.request_timeout,
                ):
                    session_bookmark = self.get_updated_session_bookmark(
                        session_bookmark, story[self.replication_key]
                    )
                    if self.is_bookmark_old(story[self.replication_key]):
                        yield story

        self.update_bookmark(session_bookmark)


Context.stream_objects["stories"] = Stories
