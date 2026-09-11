# Banquet Bar Style

**Status: recommended (interactive client & host convention).**

The **Banquet Bar** is the primary navigation and address input for Banquet interactive clients (GUI and terminal TUIs). It unifies the address bar, navigation bar, and query input into a single component functioning like a modern web browser's address bar/omnibox.

## Conventions & Rules

1. **Editable Address Bar**:
   The Banquet Bar MUST be an editable text input component positioned at the top of the interface, rather than a static, read-only display or a secondary bottom shell prompt.
2. **Default Focus**:
   The Banquet Bar MUST be focused by default upon launching the client or completing an action, allowing the user to begin typing navigation paths, commands, or queries immediately without manual refocusing.
3. **Context Reflection**:
   The value of the Banquet Bar MUST always reflect the current active context or Banquet URL. As the user navigates between containers, datasets, tables, or queries, the bar updates to reflect the canonical Banquet URL of the current view.
4. **Evaluation on Submit**:
   When the user types a new URL, command (`open`, `cd`), or relative query/clause and presses `Enter`, the client MUST evaluate the input against the active context, update the primary viewport with the resulting dataset/table/catalog, and synchronize the Banquet Bar to the newly resolved URL.
5. **Browser-like Model**:
   Rather than separating display of the current URL from a bottom CLI input prompt, interactive hosts invert this model into a browser paradigm: top address bar for input and URL display, main viewport below for content display.
