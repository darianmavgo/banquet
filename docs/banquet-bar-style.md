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
6. **History Navigation with ↑/↓**:
   The Banquet Bar MUST maintain a session history stack of evaluated inputs. Pressing `↑` recalls earlier evaluated URLs or commands; pressing `↓` navigates forward through history back to the user's unsubmitted draft input.
7. **Inline Autocomplete & Schema Suggestions**:
   The Banquet Bar MUST support schema-aware Tab completion. When typing partial paths, table names, or column names relative to the active dataset context, pressing `Tab` completes the suggestion into the bar.
8. **Dropdown Suggestion Overlay**:
   As the user types, the Banquet Bar renders a prioritized dropdown panel directly beneath the bar displaying live matches categorized with visual icons:
   - 📊 **Table**: Tables discovered in the open database schema.
   - 🔢 **Column**: Columns available in the active table.
   - 🕒 **History**: Previously evaluated Banquet queries and commands.
   - 📁 **Database**: Known local `.db` or `.sqlite` files.
   Users can traverse the dropdown via `↑`/`↓`, accept completions via `Tab`, evaluate highlighted items immediately with `Enter`, or dismiss with `Esc`.
9. **Session Back / Forward Navigation**:
   Clients MUST support browser-style session navigation using `Alt+←` / `Alt+→` (or `Ctrl+[` / `Ctrl+]` in terminals). Navigating backward re-evaluates and displays the previous URL in the session stack; navigating forward re-evaluates the subsequent URL. Divergent navigation from an earlier point in history prunes subsequent forward history.
