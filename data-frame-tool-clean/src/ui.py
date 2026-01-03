"""Tkinter app for configuring spreadsheet or CSV templates.

The UI lets users pick an Excel/CSV file, view sheet names when present,
preview headers with pandas, choose a target sheet and columns, and save
a JSON or YAML configuration next to the chosen source file.
"""
from __future__ import annotations

from pathlib import Path
from tkinter import (
    BOTH,
    END,
    LEFT,
    RIGHT,
    VERTICAL,
    Button,
    Entry,
    Frame,
    Label,
    Listbox,
    Scrollbar,
    StringVar,
    Text,
    Tk,
    filedialog,
    messagebox,
)
from tkinter import BOTH, END, LEFT, RIGHT, VERTICAL, Button, Entry, Frame, Label, Listbox, Scrollbar, StringVar, Text, Tk, filedialog, messagebox

try:  # Optional dependency
    import yaml
except ImportError:  # pragma: no cover - GUI app without tests
    yaml = None

import pandas as pd

from .templates import Template, default_template_path, describe_common_fields, save_template


def parse_skiprows(raw_value: str) -> list[int]:
    values: list[int] = []
    for part in raw_value.split(','):
        text = part.strip()
        if not text:
            continue
        values.append(int(text))
    return values


class TemplateCreatorApp(Tk):
    """Main application window for creating a sheet template."""

    def __init__(self) -> None:
        super().__init__()
        self.title("Data Frame Tool")
        self.geometry("800x600")

        self.excel_path_var = StringVar()
        self.format_var = StringVar(value="json")
        self.sheet_var = StringVar()
        self.header_row_var = StringVar(value="0")
        self.skiprows_var = StringVar(value="")
        self.delimiter_var = StringVar(value=",")
        self.encoding_var = StringVar(value="utf-8")

        self.sheet_names: list[str] = []
        self.column_names: list[str] = []

        self._build_ui()

    # UI layout helpers -------------------------------------------------
    def _build_ui(self) -> None:
        file_frame = Frame(self)
        file_frame.pack(fill="x", padx=10, pady=5)

        Label(file_frame, text="Source file:").pack(side=LEFT)
        self.file_label = Label(file_frame, textvariable=self.excel_path_var, anchor="w")
        self.file_label.pack(side=LEFT, fill="x", expand=True, padx=5)
        Button(file_frame, text="Browse", command=self.select_file).pack(side=RIGHT)

        parsing_frame = Frame(self)
        parsing_frame.pack(fill="x", padx=10, pady=5)

        Label(parsing_frame, text="Header row (0-based):").pack(side=LEFT)
        self.header_entry = Text(parsing_frame, height=1, width=5)
        self.header_entry.insert("1.0", self.header_row_var.get())
        self.header_entry.pack(side=LEFT, padx=5)
        self.header_entry.bind("<FocusOut>", lambda _e: self._sync_header_var())

        Label(parsing_frame, text="Delimiter (CSV):").pack(side=LEFT)
        self.delimiter_entry = Text(parsing_frame, height=1, width=5)
        self.delimiter_entry.insert("1.0", self.delimiter_var.get())
        self.delimiter_entry.pack(side=LEFT, padx=5)
        self.delimiter_entry.bind("<FocusOut>", lambda _e: self._sync_delimiter_var())

        Label(parsing_frame, text="Encoding (CSV):").pack(side=LEFT)
        self.encoding_entry = Text(parsing_frame, height=1, width=12)
        self.encoding_entry.insert("1.0", self.encoding_var.get())
        self.encoding_entry.pack(side=LEFT, padx=5)
        self.encoding_entry.bind("<FocusOut>", lambda _e: self._sync_encoding_var())

        sheet_frame = Frame(self)
        sheet_frame.pack(fill="both", padx=10, pady=5, expand=True)

        sheet_list_frame = Frame(sheet_frame)
        sheet_list_frame.pack(side=LEFT, fill="y", padx=(0, 10))
        Label(sheet_list_frame, text="Sheets").pack(anchor="w")

        sheet_scroll = Scrollbar(sheet_list_frame, orient=VERTICAL)
        self.sheet_list = Listbox(
            sheet_list_frame,
            listvariable=self.sheet_var,
            height=10,
            exportselection=False,
            yscrollcommand=sheet_scroll.set,
        )
        self.sheet_list.bind("<<ListboxSelect>>", self.on_sheet_selected)
        self.sheet_list.pack(side=LEFT, fill="y")
        sheet_scroll.config(command=self.sheet_list.yview)
        sheet_scroll.pack(side=RIGHT, fill="y")

        column_frame = Frame(sheet_frame)
        column_frame.pack(side=LEFT, fill="both", expand=True)
        Label(column_frame, text="Columns").pack(anchor="w")

        column_scroll = Scrollbar(column_frame, orient=VERTICAL)
        self.column_list = Listbox(
            column_frame,
            selectmode="extended",
            height=10,
            exportselection=False,
            yscrollcommand=column_scroll.set,
        )
        self.column_list.bind("<<ListboxSelect>>", self.on_columns_selected)
        self.column_list.pack(side=LEFT, fill="both", expand=True)
        column_scroll.config(command=self.column_list.yview)
        column_scroll.pack(side=RIGHT, fill="y")

        options_frame = Frame(self)
        options_frame.pack(fill="x", padx=10, pady=5)
        Label(options_frame, text="Header row (0-indexed):").pack(side=LEFT)
        Entry(options_frame, textvariable=self.header_row_var, width=6).pack(side=LEFT, padx=5)
        Label(options_frame, text="Skip rows (comma-separated):").pack(side=LEFT)
        Entry(options_frame, textvariable=self.skiprows_var, width=24).pack(side=LEFT, padx=5)

        preview_frame = Frame(self)
        preview_frame.pack(fill=BOTH, padx=10, pady=5, expand=True)

        Label(preview_frame, text="Sheet preview (first 5 rows)").pack(anchor="w")
        self.preview_box = Text(preview_frame, height=10)
        self.preview_box.pack(fill=BOTH, expand=True)

        selection_frame = Frame(self)
        selection_frame.pack(fill="x", padx=10, pady=5)

        self.selection_label = Label(selection_frame, text="No columns selected")
        self.selection_label.pack(anchor="w")

        options_frame = Frame(self)
        options_frame.pack(fill="x", padx=10, pady=5)

        Label(options_frame, text="Header row (0-indexed):").pack(side=LEFT)
        Button(options_frame, text="-", command=self._decrement_header).pack(side=LEFT, padx=2)
        Entry(options_frame, textvariable=self.header_row_var, width=5).pack(side=LEFT, padx=2)

        Label(options_frame, text="Delimiter:").pack(side=LEFT, padx=(10, 0))
        Entry(options_frame, textvariable=self.delimiter_var, width=6).pack(side=LEFT, padx=2)

        Label(options_frame, text="Encoding:").pack(side=LEFT, padx=(10, 0))
        Entry(options_frame, textvariable=self.encoding_var, width=12).pack(side=LEFT, padx=2)

        format_frame = Frame(self)
        format_frame.pack(fill="x", padx=10, pady=5)
        Label(format_frame, text="Template format:").pack(side=LEFT)
        Button(format_frame, text="JSON", command=lambda: self.set_format("json")).pack(side=LEFT, padx=2)
        Button(format_frame, text="YAML", command=lambda: self.set_format("yaml")).pack(side=LEFT, padx=2)
        Label(format_frame, text=describe_common_fields(), wraplength=520, justify="left").pack(
            side=LEFT, padx=10
        )

        action_frame = Frame(self)
        action_frame.pack(fill="x", padx=10, pady=10)
        Button(action_frame, text="Save template", command=self.save_template).pack(side=RIGHT)

    # Event handlers ----------------------------------------------------
    def set_format(self, format_name: str) -> None:
        if format_name == "yaml" and yaml is None:
            messagebox.showerror("Missing dependency", "PyYAML is not installed. Install it to save YAML templates.")
            return
        self.format_var.set(format_name)

    def select_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Select data file",
            filetypes=[
                ("Excel files", "*.xlsx *.xls *.xlsm *.xlsb"),
                ("CSV files", "*.csv"),
                ("All files", "*.*"),
            ],
        )
        if not path:
            return

        self.excel_path_var.set(path)
        self.source_type = "csv" if Path(path).suffix.lower() == ".csv" else "excel"
        if self.source_type == "csv":
            self.sheet_names = ["CSV data"]
            self.sheet_list.delete(0, END)
            self.sheet_list.insert(END, self.sheet_names[0])
            self.sheet_list.selection_set(0)
            self.load_columns(None)
        else:
            self.load_sheets(Path(path))

    def load_sheets(self, path: Path) -> None:
        self.sheet_list.delete(0, END)
        self.column_list.delete(0, END)
        self.preview_box.delete("1.0", END)

        if path.suffix.lower() == ".csv":
            self.sheet_names = []
            self.selection_label.config(text="CSV selected. Adjust parsing options then load columns.")
            self.load_columns(sheet_name=None)
            return

        try:
            workbook = pd.ExcelFile(path)
        except Exception as exc:  # pragma: no cover - handled via dialog
            messagebox.showerror("Unable to load file", f"Could not read Excel file:\n{exc}")
            return

        self.sheet_names = workbook.sheet_names
        for name in self.sheet_names:
            self.sheet_list.insert(END, name)

        self.selection_label.config(text="Select a sheet to load columns")

    def on_sheet_selected(self, event=None) -> None:
        if not self.sheet_list.curselection():
            return
        index = self.sheet_list.curselection()[0]
        sheet_name = self.sheet_names[index]
        self.load_columns(sheet_name)

    def load_columns(self, sheet_name: str | None) -> None:
        excel_path = self._current_excel_path()
        if not excel_path:
            return
        self._sync_header_var()
        self._sync_delimiter_var()
        self._sync_encoding_var()
        try:
            header_row = int(self.header_row_var.get())
        except ValueError:
            messagebox.showerror("Invalid header row", "Header row must be an integer.")
            return
        try:
            skiprows = parse_skiprows(self.skiprows_var.get())
        except ValueError:
            messagebox.showerror("Invalid skip rows", "Skip rows must be a comma-separated list of integers.")
            return
        try:
            df = pd.read_excel(
                excel_path,
                sheet_name=sheet_name,
                nrows=5,
                header=header_row,
                skiprows=skiprows,
            )
            messagebox.showerror("Invalid header", "Header row must be a non-negative integer.")
            return

        try:
            if excel_path.suffix.lower() == ".csv":
                df = pd.read_csv(
                    excel_path,
                    nrows=5,
                    header=header_row,
                    sep=self.delimiter_var.get() or ",",
                    encoding=self.encoding_var.get() or "utf-8",
                )
            else:
                df = pd.read_excel(excel_path, sheet_name=sheet_name, nrows=5, header=header_row)
        except Exception as exc:  # pragma: no cover - handled via dialog
            messagebox.showerror("Unable to read sheet", f"Could not read data: \n{exc}")
            return

        self.column_names = [str(col) for col in df.columns]
        self.column_list.delete(0, END)
        for name in self.column_names:
            self.column_list.insert(END, name)

        self.preview_box.delete("1.0", END)
        self.preview_box.insert("1.0", df.to_string(index=False))
        if sheet_name is None:
            self.selection_label.config(text="Preview loaded from CSV. Choose columns to include.")
        else:
            self.selection_label.config(text=f"Sheet selected: {sheet_name}. Choose columns to include.")

    def on_columns_selected(self, event=None) -> None:
        columns = self._selected_columns()
        if columns:
            self.selection_label.config(text=f"Selected columns: {', '.join(columns)}")
        else:
            self.selection_label.config(text="No columns selected")

    # Actions -----------------------------------------------------------
    def save_template(self) -> None:
        excel_path = self._current_excel_path()
        if not excel_path:
            messagebox.showerror("Missing data file", "Please select a data file first.")
            return

        if excel_path.suffix.lower() != ".csv" and not self.sheet_list.curselection():
            messagebox.showerror("Missing sheet", "Please select a sheet to target.")
            return

        selected_columns = self._selected_columns()
        if not selected_columns:
            messagebox.showerror("Missing columns", "Please select at least one column.")
            return
        try:
            header_row = int(self.header_row_var.get())
        except ValueError:
            messagebox.showerror("Invalid header row", "Header row must be an integer.")
            return
        try:
            skiprows = parse_skiprows(self.skiprows_var.get())
        except ValueError:
            messagebox.showerror("Invalid skip rows", "Skip rows must be a comma-separated list of integers.")
            return

        sheet_name = self.sheet_names[self.sheet_list.curselection()[0]]
        template = Template(
            sheet=sheet_name,
            header_row=header_row,
            skiprows=skiprows,
            columns=selected_columns,
            source_file=excel_path.name,
            output_dir=str(excel_path.parent),
        )
        try:
            header_row = int(self.header_row_var.get())
        except ValueError:
            messagebox.showerror("Invalid header row", "Header row must be an integer.")
            return

        sheet_name = (
            None
            if excel_path.suffix.lower() == ".csv"
            else self.sheet_names[self.sheet_list.curselection()[0]]
        )
        template = {
            "source_file": Path(excel_path).name,
            "source_type": self.source_type,
            "sheet": sheet_name,
            "header": header_row,
            "delimiter": self.delimiter_var.get() or ",",
            "encoding": self.encoding_var.get() or "utf-8",
            "columns": selected_columns,
        }

        output_format = self.format_var.get()
        suffix = "yaml" if output_format == "yaml" else "json"
        output_path = default_template_path(excel_path, suffix=suffix)

        try:
            if output_format == "yaml" and yaml is None:
                raise RuntimeError("PyYAML is required for YAML output")

            save_template(template, output_path)
        except Exception as exc:  # pragma: no cover - handled via dialog
            messagebox.showerror("Save failed", f"Could not save template:\n{exc}")
            return

        messagebox.showinfo(
            "Template saved",
            f"Template written to:\n{output_path}\n\n{describe_common_fields()}",
        )

    # Helpers -----------------------------------------------------------
    def _decrement_header(self) -> None:
        try:
            current = int(self.header_row_var.get())
        except ValueError:
            current = 0
        self.header_row_var.set(str(max(0, current - 1)))

    def _selected_columns(self) -> list[str]:
        indices = self.column_list.curselection()
        return [self.column_names[i] for i in indices]

    def _current_excel_path(self) -> Path | None:
        value = self.excel_path_var.get()
        return Path(value) if value else None

    def _sync_header_var(self) -> None:
        value = self.header_entry.get("1.0", END).strip()
        self.header_row_var.set(value or "0")

    def _sync_delimiter_var(self) -> None:
        value = self.delimiter_entry.get("1.0", END).strip()
        self.delimiter_var.set(value or ",")

    def _sync_encoding_var(self) -> None:
        value = self.encoding_entry.get("1.0", END).strip()
        self.encoding_var.set(value)


def main() -> None:
    app = TemplateCreatorApp()
    app.mainloop()


if __name__ == "__main__":
    main()
