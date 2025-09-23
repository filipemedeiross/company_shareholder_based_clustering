document.addEventListener("DOMContentLoaded", () => {
    const searchInput = document.querySelector("input[name='q']");
    const message     = "Please fill out this field.";

    const setMessage   = () => searchInput.setCustomValidity(message);
    const clearMessage = () => searchInput.setCustomValidity("");

    searchInput.addEventListener("invalid", e => {
        e.preventDefault();
        setMessage();
        searchInput.reportValidity();
    });
    searchInput.addEventListener("mouseover", () => {
        if (!searchInput.value) setMessage();
    });

    searchInput.addEventListener("input"   , clearMessage);
    searchInput.addEventListener("mouseout", clearMessage);
});