-- Remove a leading level-1 heading so it isn't duplicated by the
-- <h1 class="title">$title$</h1> already rendered in the page header.
function Pandoc(doc)
  local first = doc.blocks[1]
  if first and first.t == "Header" and first.level == 1 then
    table.remove(doc.blocks, 1)
  end
  return doc
end
