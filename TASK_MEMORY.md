# Task Memory

**Created:** 2025-07-10 22:23:35
**Branch:** feature/task-concurrency-control

## Requirements

# Task concurrency control

**Labels:** enhancement, devx

**Issue URL:** https://github.com/chrisguidry/docket/issues/86

## Description

A very common requirement of background task systems is to govern concurrent access based on aspects of the data, like "only run one task per unique `customer_id` at the same time" or "only run `n` tasks per database name at a time".  This should be separate from the `key` of a task and should be expressed in more granular dependencies/parameter annotations.


## Development Notes

*Update this section as you work on the task. Include:*
- *Progress updates*
- *Key decisions made*
- *Challenges encountered*
- *Solutions implemented*
- *Files modified*
- *Testing notes*

### Work Log

- [2025-07-10 22:23:35] Task setup completed, TASK_MEMORY.md created

---

*This file serves as your working memory for this task. Keep it updated as you progress through the implementation.*
