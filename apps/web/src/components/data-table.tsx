import { useState, useRef, useEffect } from "react"
import {
  type ColumnDef,
  type ColumnFiltersState,
  type RowData,
  type SortingState,
  type ColumnVisibilityState,
  type RowSelectionState,
  columnFilteringFeature,
  columnSizingFeature,
  columnVisibilityFeature,
  createFilteredRowModel,
  createPaginatedRowModel,
  createSortedRowModel,
  filterFns,
  flexRender,
  rowPaginationFeature,
  rowSelectionFeature,
  rowSortingFeature,
  sortFns,
  tableFeatures,
  useTable,
} from "@tanstack/react-table"
import { useVirtualizer } from "@tanstack/react-virtual"
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  DropdownMenu, DropdownMenuCheckboxItem, DropdownMenuContent,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Checkbox } from "@/components/ui/checkbox"
import { ChevronLeft, ChevronRight, Columns3, Search } from "lucide-react"

const ROW_HEIGHT = 36

// TanStack Table v9 only exposes APIs for explicitly registered features.
// The full built-in filter/sort registries keep v8's `"auto"` fn resolution.
const dataTableFeatures = tableFeatures({
  columnFilteringFeature,
  columnSizingFeature,
  columnVisibilityFeature,
  rowPaginationFeature,
  rowSelectionFeature,
  rowSortingFeature,
  filteredRowModel: createFilteredRowModel(),
  sortedRowModel: createSortedRowModel(),
  paginatedRowModel: createPaginatedRowModel(),
  filterFns,
  sortFns,
})

export type DataTableFeatures = typeof dataTableFeatures
export type DataTableColumnDef<TData extends RowData> = ColumnDef<DataTableFeatures, TData>

interface DataTableProps<TData extends RowData> {
  columns: DataTableColumnDef<TData>[]
  data: TData[]
  searchKey?: string
  searchPlaceholder?: string
  onRowClick?: (row: TData) => void
  pageSize?: number
  enableVirtualization?: boolean
  enableSelection?: boolean
  onSelectionChange?: (selectedRows: TData[]) => void
  /** Default visibility for columns (e.g. hide rich/optional columns by default). */
  initialColumnVisibility?: ColumnVisibilityState
  /** localStorage key to persist column visibility across reloads. */
  columnVisibilityKey?: string
}

export function DataTable<TData extends RowData>({
  columns,
  data,
  searchKey,
  searchPlaceholder = "Search...",
  onRowClick,
  pageSize = 500,
  enableVirtualization = true,
  enableSelection = false,
  onSelectionChange,
  initialColumnVisibility,
  columnVisibilityKey,
}: DataTableProps<TData>) {
  const [sorting, setSorting] = useState<SortingState>([])
  const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([])
  const [columnVisibility, setColumnVisibility] = useState<ColumnVisibilityState>(() => {
    if (columnVisibilityKey) {
      try {
        const saved = localStorage.getItem(columnVisibilityKey)
        if (saved) return JSON.parse(saved)
      } catch { /* ignore */ }
    }
    return initialColumnVisibility ?? {}
  })
  const [rowSelection, setRowSelection] = useState<RowSelectionState>({})
  const tableContainerRef = useRef<HTMLDivElement>(null)

  const previousData = useRef(data)
  useEffect(() => {
    const previous = previousData.current
    previousData.current = data
    // Index-based selections must never move to different backing records.
    if (Object.keys(rowSelection).some(key => rowSelection[key] && previous[Number(key)] !== data[Number(key)])) {
      setRowSelection({})
      onSelectionChange?.([])
    }
  }, [data, rowSelection, onSelectionChange])

  // Prepend select column if enabled
  const allColumns = enableSelection
    ? [
        {
          id: "select",
          header: ({ table: t }) => (
            <Checkbox
              checked={t.getIsAllPageRowsSelected()}
              onCheckedChange={(value) => t.toggleAllPageRowsSelected(!!value)}
              aria-label="Select all"
              className="translate-y-[2px]"
            />
          ),
          cell: ({ row }) => (
            <Checkbox
              checked={row.getIsSelected()}
              onCheckedChange={(value) => row.toggleSelected(!!value)}
              aria-label="Select row"
              className="translate-y-[2px]"
              onClick={(e: React.MouseEvent) => e.stopPropagation()}
            />
          ),
          enableSorting: false,
          enableHiding: false,
          size: 32,
        } satisfies DataTableColumnDef<TData>,
        ...columns,
      ]
    : columns

  const table = useTable({
    features: dataTableFeatures,
    data,
    columns: allColumns,
    // Only paginate client-side when NOT virtualizing. Manual pagination passes
    // every row through, matching v8 without a pagination row model.
    manualPagination: enableVirtualization,
    onSortingChange: setSorting,
    onColumnFiltersChange: setColumnFilters,
    onColumnVisibilityChange: (updater) => {
      setColumnVisibility((prev) => {
        const next = typeof updater === "function" ? updater(prev) : updater
        if (columnVisibilityKey) {
          try { localStorage.setItem(columnVisibilityKey, JSON.stringify(next)) } catch { /* ignore */ }
        }
        return next
      })
    },
    onRowSelectionChange: (updater) => {
      setRowSelection(updater)
      // Notify parent of selection changes
      if (onSelectionChange) {
        const next = typeof updater === "function" ? updater(rowSelection) : updater
        const selectedRows = Object.keys(next)
          .filter((k) => next[k])
          .map((k) => data[parseInt(k)])
          .filter(Boolean)
        onSelectionChange(selectedRows)
      }
    },
    initialState: {
      pagination: { pageIndex: 0, pageSize },
    },
    state: {
      sorting,
      columnFilters,
      columnVisibility,
      rowSelection,
    },
  })

  const { rows } = table.getRowModel()

  // Virtualizer — only active when enableVirtualization is true
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => tableContainerRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 20,
  })

  const virtualRows = enableVirtualization ? virtualizer.getVirtualItems() : null
  const totalSize = enableVirtualization ? virtualizer.getTotalSize() : 0

  const selectedCount = table.getFilteredSelectedRowModel().rows.length

  return (
    <div className="flex flex-col gap-2 h-full">
      {/* Toolbar */}
      <div className="flex items-center gap-2">
        {searchKey && (
          <div className="relative max-w-sm">
            <Search className="absolute left-2.5 top-2.5 size-4 text-muted-foreground" />
            <Input
              placeholder={searchPlaceholder}
              value={(table.getColumn(searchKey)?.getFilterValue() as string) ?? ""}
              onChange={(event) =>
                table.getColumn(searchKey)?.setFilterValue(event.target.value)
              }
              className="pl-8"
            />
          </div>
        )}
        <div className="ml-auto flex items-center gap-2">
          {selectedCount > 0 && (
            <div className="text-xs font-medium">
              {selectedCount} selected
            </div>
          )}
          <div className="text-xs text-muted-foreground">
            {table.getFilteredRowModel().rows.length} rows
          </div>
          <DropdownMenu>
            <DropdownMenuTrigger>
              <span className="inline-flex items-center gap-1.5 rounded-md border bg-background px-3 py-1.5 text-sm font-medium shadow-xs hover:bg-accent hover:text-accent-foreground transition-colors">
                <Columns3 className="size-4" />
                Columns
              </span>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              {table
                .getAllColumns()
                .filter((column) => column.getCanHide())
                .map((column) => (
                  <DropdownMenuCheckboxItem
                    key={column.id}
                    className="capitalize"
                    checked={column.getIsVisible()}
                    onCheckedChange={(value) => column.toggleVisibility(!!value)}
                  >
                    {column.id}
                  </DropdownMenuCheckboxItem>
                ))}
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </div>

      {/* Table */}
      <div
        ref={tableContainerRef}
        className="flex-1 min-h-0 rounded-md border overflow-auto"
      >
        <Table>
          <TableHeader className="sticky top-0 z-10 bg-background">
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id}>
                {headerGroup.headers.map((header) => (
                  <TableHead key={header.id} style={{ width: header.getSize() }}>
                    {header.isPlaceholder
                      ? null
                      : flexRender(header.column.columnDef.header, header.getContext())}
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {enableVirtualization && virtualRows ? (
              <>
                {/* Top spacer */}
                {virtualRows.length > 0 && virtualRows[0].start > 0 && (
                  <tr style={{ height: virtualRows[0].start }} />
                )}
                {virtualRows.map((virtualRow) => {
                  const row = rows[virtualRow.index]
                  return (
                    <TableRow
                      key={row.id}
                      data-state={row.getIsSelected() && "selected"}
                      data-index={virtualRow.index}
                      className={onRowClick ? "cursor-pointer" : ""}
                      onClick={() => onRowClick?.(row.original)}
                      style={{ height: ROW_HEIGHT }}
                    >
                      {row.getVisibleCells().map((cell) => (
                        <TableCell key={cell.id}>
                          {flexRender(cell.column.columnDef.cell, cell.getContext())}
                        </TableCell>
                      ))}
                    </TableRow>
                  )
                })}
                {/* Bottom spacer */}
                {virtualRows.length > 0 && (
                  <tr style={{ height: totalSize - virtualRows[virtualRows.length - 1].end }} />
                )}
              </>
            ) : rows.length ? (
              rows.map((row) => (
                <TableRow
                  key={row.id}
                  data-state={row.getIsSelected() && "selected"}
                  className={onRowClick ? "cursor-pointer" : ""}
                  onClick={() => onRowClick?.(row.original)}
                >
                  {row.getVisibleCells().map((cell) => (
                    <TableCell key={cell.id}>
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={allColumns.length} className="h-24 text-center text-muted-foreground">
                  No results.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>

      {/* Footer */}
      <div className="flex items-center justify-between shrink-0 py-1">
        <div className="text-xs text-muted-foreground">
          {selectedCount > 0 && (
            <>{selectedCount} of{" "}</>
          )}
          {table.getFilteredRowModel().rows.length} row(s)
          {enableVirtualization && rows.length > 100 && (
            <span className="ml-2 text-muted-foreground/60">· virtualized</span>
          )}
        </div>
        {!enableVirtualization && (
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => table.previousPage()}
              disabled={!table.getCanPreviousPage()}
            >
              <ChevronLeft className="size-4" />
              Previous
            </Button>
            <div className="text-xs text-muted-foreground">
              Page {table.state.pagination.pageIndex + 1} of {table.getPageCount()}
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={() => table.nextPage()}
              disabled={!table.getCanNextPage()}
            >
              Next
              <ChevronRight className="size-4" />
            </Button>
          </div>
        )}
      </div>
    </div>
  )
}
