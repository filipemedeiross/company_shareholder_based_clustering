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

document.addEventListener('DOMContentLoaded', function() {
    const sidebar       = document.getElementById('sidebar'       );
    const sidebarToggle = document.getElementById('sidebar-toggle');
    const toggleIcon    = document.getElementById('toggle-icon'   );
    const menuTexts     = document.querySelectorAll('.menu-text'  );
    const mainContent   = document.getElementById('main-content'  );

    const sidebarCollapsed = localStorage.getItem('sidebarCollapsed') === 'true';

    if (sidebarCollapsed) {
        sidebar      .classList.add('collapsed');
        sidebarToggle.classList.add('collapsed');
        toggleIcon.classList.remove('fa-chevron-left' );
        toggleIcon.classList.add   ('fa-chevron-right');

        menuTexts.forEach(text => text.style.display = 'none');

        if (mainContent) {
            mainContent.classList.add('expanded' );
        }
    }

    sidebarToggle.addEventListener('click', function() {
        sidebar      .classList.toggle('collapsed');
        sidebarToggle.classList.toggle('collapsed');

        if (mainContent) {
            mainContent.classList.toggle('expanded' );
        }

        if (sidebar.classList.contains('collapsed')) {
            toggleIcon.classList.remove('fa-chevron-left' );
            toggleIcon.classList.add   ('fa-chevron-right');

            menuTexts.forEach(text => text.style.display = 'none');

            localStorage.setItem('sidebarCollapsed', 'true');
        } else {
            toggleIcon.classList.remove('fa-chevron-right');
            toggleIcon.classList.add   ('fa-chevron-left' );

            menuTexts.forEach(text => text.style.display = 'inline');

            localStorage.setItem('sidebarCollapsed', 'false');
        }
    });
});