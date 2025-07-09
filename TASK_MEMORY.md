# Task Memory

**Created:** 2025-07-09 15:27:34
**Branch:** feature/more-queue-health

## Requirements

# More queue health details from the CLI

**Issue URL:** https://github.com/chrisguidry/docket/issues/146

## Description

## An Issue with a Curious Lack of Details

I know there's the `docket snapshot` command and there's also metrics collection, but I was trying to debug a runaway task earlier and was hoping for a way to see higher-level stats on tasks, like # of a specific task enqueued.

I first suspected I'd created a task that ended up reproducing itself, so I was trying to look around and guess that. But it was just a while loop that didn't break out correctly (wrong Redis key structure! 😂). Anyway, I was creating hundreds of tasks and some way to see a rollup on function would have been nice, so I could do a quick verify on whether that task was going crazy.

Curious what you folks think about that or if you'd rather avoid more work in the 'snapshot' area and push that to observation stacks, etc. Tracing might have helped, but I don't have my otel stack set up yet for these projects. 😅 


## Development Notes

*Update this section as you work on the task. Include:*
- *Progress updates*
- *Key decisions made*
- *Challenges encountered*
- *Solutions implemented*
- *Files modified*
- *Testing notes*

### Work Log

- [2025-07-09 15:27:34] Task setup completed, TASK_MEMORY.md created

---

*This file serves as your working memory for this task. Keep it updated as you progress through the implementation.*
